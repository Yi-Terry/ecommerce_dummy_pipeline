create table if not exists workspace.silver.ecommerce_events (
    event_id string,
    event_type string,
    session_id string,
    user_id string,
    device string,
    referrer string,
    page string,
    product_id string,
    product_name string,
    category string,
    price float,
    quantity integer,
    cart_size integer,
    cart_value float,
    order_id string,
    order_value float,
    event_timestamp timestamp_ntz
);

MERGE INTO workspace.silver.ecommerce_events AS target
USING (
    SELECT
        event_id,
        get_json_object(raw_json, '$.event_type') AS event_type,
        get_json_object(raw_json, '$.session_id') AS session_id,
        get_json_object(raw_json, '$.user_id') AS user_id,
        get_json_object(raw_json, '$.device') AS device,
        get_json_object(raw_json, '$.referrer') AS referrer,
        get_json_object(raw_json, '$.page') AS page,
        get_json_object(raw_json, '$.product_id') AS product_id,
        get_json_object(raw_json, '$.product_name') AS product_name,
        get_json_object(raw_json, '$.category') AS category,
        CAST(get_json_object(raw_json, '$.price') AS FLOAT) AS price,
        CAST(get_json_object(raw_json, '$.quantity') AS INTEGER) AS quantity,
        CAST(get_json_object(raw_json, '$.cart_size') AS INTEGER) AS cart_size,
        CAST(get_json_object(raw_json, '$.cart_value') AS FLOAT) AS cart_value,
        get_json_object(raw_json, '$.order_id') AS order_id,
        CAST(get_json_object(raw_json, '$.order_value') AS FLOAT) AS order_value,
        CAST(event_timestamp AS TIMESTAMP_NTZ) AS event_timestamp
    FROM workspace.bronze.ecommerce_events
) AS source
ON target.event_id = source.event_id
WHEN NOT MATCHED THEN INSERT *