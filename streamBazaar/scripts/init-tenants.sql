CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS tenants (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    priority_weight FLOAT NOT NULL,
    virtual_currency_balance BIGINT NOT NULL,
    sla_requirements JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS operators (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID REFERENCES tenants(id),
    operator_type VARCHAR(100) NOT NULL,
    resource_requirements JSONB NOT NULL,
    placement_node VARCHAR(255)
);
