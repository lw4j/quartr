import pytest

from app.companies.cik_mapping import normalize_cik
from app.reports.identity import ReportIdentity
from app.reports.state import ReportState, assert_transition, InvalidStateTransition
from app.sec.submissions import FilingRecord, FilingSelector, select_latest
from app.sec.retry import PermanentSecError
from app.storage.artifact_store import ArtifactStore, InvalidArtifactRefError
from datetime import date


def test_normalize_cik():
    assert normalize_cik(320193) == "0000320193"
    assert normalize_cik("320193") == "0000320193"
    assert normalize_cik("0000320193") == "0000320193"


def test_report_identity_logical_path():
    identity = ReportIdentity(ticker="aapl", form="10-k", year=2025)
    assert identity.logical_path == "/AAPL/10-K/2025"

    identity_q = ReportIdentity(ticker="AAPL", form="10-Q", year=2026, quarter="Q1")
    assert identity_q.logical_path == "/AAPL/10-Q/2026/Q1"


def test_report_identity_parse_roundtrip():
    identity = ReportIdentity.parse("/AAPL/10-K/2025")
    assert identity.ticker == "AAPL"
    assert identity.form == "10-K"
    assert identity.year == 2025


def test_amended_form_does_not_introduce_a_path_segment():
    """SEC writes amendments as `10-K/A`, whose slash would otherwise
    collide with the logical-path delimiter and land in the quarter slot,
    making the path unparseable. It is encoded the same way the artifact
    store encodes it, so the path stays three segments and round-trips.
    """
    identity = ReportIdentity(ticker="AAPL", form="10-K/A", year=2025)

    assert identity.form == "10-K/A"
    assert identity.logical_path == "/AAPL/10-K_A/2025"

    parsed = ReportIdentity.parse(identity.logical_path)
    assert parsed.form == "10-K/A"
    assert parsed.year == 2025
    assert parsed.quarter is None


def test_state_transitions_valid_and_invalid():
    assert_transition(ReportState.ACCEPTED, ReportState.QUEUED)
    assert_transition(ReportState.QUEUED, ReportState.PROCESSING)
    assert_transition(ReportState.PROCESSING, ReportState.COMPLETED)
    with pytest.raises(InvalidStateTransition):
        assert_transition(ReportState.COMPLETED, ReportState.PROCESSING)


def _filing(form: str, filing_date: str, accession: str) -> FilingRecord:
    return FilingRecord(
        cik="0000320193",
        form=form,
        filing_date=date.fromisoformat(filing_date),
        accession_number=accession,
        primary_document="doc.htm",
        company_name="Apple Inc.",
    )


def test_select_latest_excludes_amendments_by_default():
    filings = [
        _filing("10-K", "2024-11-01", "0000320193-24-000001"),
        _filing("10-K", "2025-11-01", "0000320193-25-000002"),
        _filing("10-K/A", "2025-12-01", "0000320193-25-000099"),
    ]
    selector = FilingSelector(form="10-K", include_amended=False)
    latest = select_latest(filings, selector)
    assert latest.accession_number == "0000320193-25-000002"


def test_select_latest_includes_amendments_when_requested():
    filings = [
        _filing("10-K", "2025-11-01", "0000320193-25-000002"),
        _filing("10-K/A", "2025-12-01", "0000320193-25-000099"),
    ]
    selector = FilingSelector(form="10-K", include_amended=True)
    latest = select_latest(filings, selector)
    assert latest.accession_number == "0000320193-25-000099"


def test_select_latest_raises_permanent_error_when_missing():
    selector = FilingSelector(form="10-K")
    with pytest.raises(PermanentSecError):
        select_latest([], selector)


def test_artifact_ref_round_trips():
    store = ArtifactStore(storage_root="/data/reports")
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)
    ref = store.ref_for(identity, "0000320193-25-000079")

    assert ref == "AAPL/10-K/2025/0000320193-25-000079"
    # The public ref must not leak the storage backend.
    assert "/data/reports" not in ref

    parsed_identity, accession = store.parse_ref(ref)
    assert parsed_identity == identity
    assert accession == "0000320193-25-000079"


def test_artifact_ref_round_trips_amended_form():
    store = ArtifactStore(storage_root="/data/reports")
    identity = ReportIdentity(ticker="AAPL", form="10-K/A", year=2025)
    ref = store.ref_for(identity, "0000320193-25-000079")

    # The '/' in the form must not create an extra path segment.
    assert ref == "AAPL/10-K_A/2025/0000320193-25-000079"
    assert store.parse_ref(ref)[0] == identity


def test_artifact_ref_resolves_inside_storage_root():
    store = ArtifactStore(storage_root="/data/reports")
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)
    ref = store.ref_for(identity, "0000320193-25-000079")

    assert store.resolve_pdf_path(ref) == (
        "/data/reports/AAPL/10-K/2025/accession-0000320193-25-000079/report.pdf"
    )


@pytest.mark.parametrize(
    "accession",
    [
        "0000320193-25-000079",  # conventional 6-digit sequence
        "0000320193-25-1",       # sequence width is not normatively guaranteed
        "0000320193-25-0000001",
    ],
)
def test_artifact_ref_accepts_any_sequence_width(accession):
    """The 10-2 prefix is documented; the sequence length is not.

    Pinning a fixed width would reject legitimate accessions, so only the
    prefix shape and digit-ness are enforced.
    """
    store = ArtifactStore(storage_root="/data/reports")
    identity = ReportIdentity(ticker="AAPL", form="10-K", year=2025)

    ref = store.ref_for(identity, accession)
    assert store.parse_ref(ref) == (identity, accession)


@pytest.mark.parametrize(
    "bad_ref",
    [
        "AAPL/10-K/2025/../../../etc/passwd",
        "../../etc/passwd",
        "AAPL/10-K/2025/not-an-accession",
        "AAPL/10-K/2025/000032019-25-000079",   # 9-digit filer prefix
        "AAPL/10-K/2025/0000320193-2025-000079",  # 4-digit year
        "AAPL/10-K/2025/0000320193-25-",          # empty sequence
        "AAPL/10-K/2025",
        "",
    ],
)
def test_artifact_ref_rejects_malformed_and_traversal(bad_ref):
    store = ArtifactStore(storage_root="/data/reports")
    with pytest.raises(InvalidArtifactRefError):
        store.resolve_pdf_path(bad_ref)


def test_explicit_amended_form_never_matches_the_original():
    """`form: "10-K/A"` asks for the amendment specifically.

    The worker previously passed `form.split("/")[0]` to the selector, which
    silently downgraded the request to a plain `10-K` and returned the
    original filing -- a wrong document reported as success.
    """
    from app.sec.submissions import FilingSelector

    amended = FilingSelector(form="10-K/A", include_amended=False)
    assert amended.matches("10-K/A")
    assert not amended.matches("10-K")

    # include_amended is irrelevant once the form is already explicit.
    assert not FilingSelector(form="10-K/A", include_amended=True).matches("10-K")

    # The base form keeps its existing behaviour.
    base = FilingSelector(form="10-K", include_amended=False)
    assert base.matches("10-K") and not base.matches("10-K/A")
    widened = FilingSelector(form="10-K", include_amended=True)
    assert widened.matches("10-K") and widened.matches("10-K/A")


def test_filing_without_a_primary_document_is_not_selectable():
    """SEC sometimes omits `primaryDocument`.

    The Archives URL for such a record ends in a slash and resolves to the
    accession's directory listing, which is a real HTML page: it converts
    to a valid PDF of a file index and the task reports success with the
    wrong document. Selection excludes it so the request fails honestly.
    """
    from datetime import date

    from app.sec.retry import PermanentSecError
    from app.sec.submissions import FilingRecord, FilingSelector, select_latest

    def record(accession, primary_document, day):
        return FilingRecord(
            cik="0000320193", form="10-K", filing_date=date(2025, 10, day),
            accession_number=accession, primary_document=primary_document,
            company_name="Apple Inc.",
        )

    selector = FilingSelector(form="10-K", include_amended=False)

    # The newest filing is unusable, so the older complete one wins.
    usable = record("0000320193-25-000079", "aapl-20250927.htm", 1)
    chosen = select_latest([usable, record("0000320193-25-000999", "", 31)], selector)
    assert chosen.accession_number == "0000320193-25-000079"

    # Nothing usable at all is a permanent failure, not a directory listing.
    with pytest.raises(PermanentSecError):
        select_latest([record("0000320193-25-000999", "", 31)], selector)
