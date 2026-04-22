import json
import os
from pathlib import Path

import psycopg
import yaml


def main() -> None:
    cfg_file = Path("configs/tenant-configs/sample-tenants.yml")
    data = yaml.safe_load(cfg_file.read_text(encoding="utf-8"))

    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "15432")),
        dbname=os.getenv("POSTGRES_DB", "streamBazaar"),
        user=os.getenv("POSTGRES_USER", "sb_user"),
        password=os.getenv("POSTGRES_PASSWORD", "sb_pass"),
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute(Path("scripts/init-tenants.sql").read_text(encoding="utf-8"))
            for tenant in data.get("tenants", []):
                cur.execute(
                    """
                    INSERT INTO tenants (name, priority_weight, virtual_currency_balance, sla_requirements)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        tenant["name"],
                        tenant["priority_weight"],
                        int(tenant["virtual_currency_balance"]),
                        json.dumps(tenant["sla_requirements"]),
                    ),
                )

    print("Tenant configuration initialized.")


if __name__ == "__main__":
    main()
