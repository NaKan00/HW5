FROM grafana/grafana:11.6.0
USER root
ADD --checksum=sha256:d2df55da4cc88e666620f8571c795d7ae26af7abb504410cf9a470d143058b7f https://github.com/grafana/clickhouse-datasource/releases/download/v4.8.2/grafana-clickhouse-datasource-4.8.2.zip /tmp/clickhouse.zip
RUN mkdir -p /opt/grafana-plugins /opt/grafana-dashboards \
    && unzip -q /tmp/clickhouse.zip -d /opt/grafana-plugins \
    && rm /tmp/clickhouse.zip
ENV GF_PATHS_PLUGINS=/opt/grafana-plugins
COPY --chmod=644 provisioning/datasources/*.yml /etc/grafana/provisioning/datasources/
COPY --chmod=644 provisioning/dashboards/*.yml /etc/grafana/provisioning/dashboards/
COPY --chmod=644 dashboards/*.json /opt/grafana-dashboards/
USER 472
