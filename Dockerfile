FROM python:3.14-slim

# WeasyPrint renders through Pango/HarfBuzz; it dropped the cairo and
# gdk-pixbuf dependencies in 53+, so the old libcairo2/libgdk-pixbuf packages
# are no longer needed (and libgdk-pixbuf-xlib no longer exists on trixie).
# fonts-dejavu-core gives the container a real font so PDFs aren't blank.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpango-1.0-0 libpangoft2-1.0-0 libharfbuzz-subset0 \
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /srv/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

ENV PYTHONUNBUFFERED=1
