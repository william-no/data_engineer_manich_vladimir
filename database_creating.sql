
CREATE SCHEMA log;

CREATE TABLE log.users (
    user_id uuid PRIMARY KEY,
    username VARCHAR(50),
    email VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE log.topics (
    id uuid PRIMARY KEY,
    user_id uuid REFERENCES log.users(user_id),
    title TEXT NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE log.messages (
    id uuid PRIMARY KEY,
    topic_id uuid REFERENCES log.topics(id),
    user_id uuid REFERENCES log.users(user_id) ON DELETE SET NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TYPE log.enum_event_type AS ENUM (
    'first_visit',
    'registration',
    'login',
    'logout',
    'create_topic',
    'view_topic',
    'delete_topic',
    'write_message'
);

CREATE TYPE log.enum_object_type AS ENUM (
    'topic',
    'message'
);


CREATE TABLE log.logs (
    user_id uuid REFERENCES log.users(user_id) NOT NULL,
    event_type log.enum_event_type NOT NULL,
    object_type log.enum_object_type,
    object_id uuid,
    server_response VARCHAR(10) CHECK (server_response IN ('success', 'error')) NOT NULL,
    response_details JSONB,
    log_time TIMESTAMP DEFAULT NOW() NOT NULL,
    primary key (user_id, log_time)
) PARTITION BY RANGE (log_time);

CREATE TABLE log.logs_2026_05 PARTITION OF log.logs
FOR VALUES FROM ('2026-05-01 00:00:00+00') TO ('2026-06-01 00:00:00+00');
CREATE TABLE log.logs_2026_06 PARTITION OF log.logs
FOR VALUES FROM ('2026-06-01 00:00:00+00') TO ('2026-07-01 00:00:00+00');
CREATE TABLE log.logs_2026_07 PARTITION OF log.logs
FOR VALUES FROM ('2026-07-01 00:00:00+00') TO ('2026-08-01 00:00:00+00');
CREATE TABLE log.logs_2026_08 PARTITION OF log.logs
FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-09-01 00:00:00+00');



CREATE INDEX idx_logs_error_analysis ON log.logs(server_response, log_time, user_id);
CREATE INDEX idx_logs_object_audit ON log.logs(object_type, object_id, log_time);
CREATE INDEX idx_logs_first_visit ON log.logs(log_time DESC)
WHERE event_type = 'first_visit';
CREATE INDEX idx_logs_latest_event ON log.logs(event_type, log_time DESC);


