from base64 import b64encode

from common import GRAFANA, demo_grafana_password, http, require, wait

DASHBOARD_UID = "cinema-analytics"


def main():
    wait("Grafana", lambda: http(f"{GRAFANA}/api/health"))
    credentials = b64encode(f"admin:{demo_grafana_password()}".encode()).decode()
    headers = {"Authorization": f"Basic {credentials}"}
    provisioned = http(f"{GRAFANA}/api/dashboards/uid/{DASHBOARD_UID}", headers=headers)
    dashboard = provisioned["dashboard"]
    require(provisioned["meta"]["provisioned"], "Dashboard was not provisioned")
    datasource = http(f"{GRAFANA}/api/datasources/uid/cinema-clickhouse", headers=headers)
    require(datasource["type"] == "grafana-clickhouse-datasource", "Unexpected datasource")
    require(len(dashboard["panels"]) == 5, "Expected five dashboard panels")

    for panel in dashboard["panels"]:
        result = http(
            f"{GRAFANA}/api/ds/query",
            headers=headers,
            data={
                "from": "1775001600000",  # 2026-04-01T00:00:00Z
                "to": "1775779200000",    # 2026-04-10T00:00:00Z
                "queries": panel["targets"],
            },
        )["results"]["A"]
        frames = result.get("frames")
        require(frames and frames[0]["data"]["values"][0], f"No data from {panel['title']}: {result}")
        if panel["id"] == 1:
            columns = [field["name"] for field in frames[0]["schema"]["fields"]]
            values = frames[0]["data"]["values"]
            index = values[columns.index("Cohort")].index("2026-04-01")
            rates = [values[columns.index(f"Day {day}")][index] for day in range(8)]
            require(rates == [1, 0.5, 0, 0, 0, 0, 0, 0.3], f"Grafana returned retention {rates}")
        print(f"Grafana query verified: {panel['title']}")
    print("Grafana provisioning demonstration passed.")


if __name__ == "__main__":
    main()
