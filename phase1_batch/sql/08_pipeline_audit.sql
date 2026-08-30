
SET NOCOUNT ON;
GO

IF SCHEMA_ID('etl') IS NULL
    EXEC('CREATE SCHEMA etl;');
GO

IF OBJECT_ID('etl.pipeline_run_audit', 'U') IS NULL
BEGIN
    CREATE TABLE etl.pipeline_run_audit
    (
        audit_id          BIGINT IDENTITY(1,1) NOT NULL,
        run_id            NVARCHAR(100) NOT NULL,
        pipeline_name     NVARCHAR(200) NOT NULL,
        source_name       NVARCHAR(128) NOT NULL,
        activity_name     NVARCHAR(200) NULL,

        run_start_at      DATETIME2(0) NOT NULL,
        run_end_at        DATETIME2(0) NULL,

        rows_read         BIGINT NULL,
        rows_written      BIGINT NULL,

        status            NVARCHAR(20) NOT NULL
                          CONSTRAINT DF_pipeline_audit_status DEFAULT 'RUNNING',

        error_message     NVARCHAR(2000) NULL,

        created_at        DATETIME2(0) NOT NULL
                          CONSTRAINT DF_pipeline_audit_created DEFAULT SYSUTCDATETIME(),

        updated_at        DATETIME2(0) NOT NULL
                          CONSTRAINT DF_pipeline_audit_updated DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_pipeline_run_audit PRIMARY KEY (audit_id),

        CONSTRAINT UQ_pipeline_run_audit_run_source
            UNIQUE (run_id, source_name),

        CONSTRAINT CK_pipeline_run_audit_status
            CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED'))
    );

    CREATE INDEX IX_pipeline_run_audit_run_id
        ON etl.pipeline_run_audit(run_id);

    CREATE INDEX IX_pipeline_run_audit_status_start
        ON etl.pipeline_run_audit(status, run_start_at);
END;
GO

CREATE OR ALTER PROCEDURE etl.usp_audit_run_start
    @run_id        NVARCHAR(100),
    @pipeline_name NVARCHAR(200),
    @source_name   NVARCHAR(128),
    @activity_name NVARCHAR(200) = NULL,
    @run_start_at  DATETIME2(0)
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS
    (
        SELECT 1
        FROM etl.pipeline_run_audit
        WHERE run_id = @run_id
          AND source_name = @source_name
    )
    BEGIN
        UPDATE etl.pipeline_run_audit
        SET pipeline_name = @pipeline_name,
            activity_name = @activity_name,
            run_start_at = @run_start_at,
            run_end_at = NULL,
            rows_read = NULL,
            rows_written = NULL,
            status = 'RUNNING',
            error_message = NULL,
            updated_at = SYSUTCDATETIME()
        WHERE run_id = @run_id
          AND source_name = @source_name;
    END
    ELSE
    BEGIN
        INSERT INTO etl.pipeline_run_audit
        (
            run_id,
            pipeline_name,
            source_name,
            activity_name,
            run_start_at,
            status
        )
        VALUES
        (
            @run_id,
            @pipeline_name,
            @source_name,
            @activity_name,
            @run_start_at,
            'RUNNING'
        );
    END;
END;
GO

CREATE OR ALTER PROCEDURE etl.usp_audit_run_complete
    @run_id        NVARCHAR(100),
    @source_name   NVARCHAR(128),
    @status        NVARCHAR(20),
    @rows_read     BIGINT = NULL,
    @rows_written  BIGINT = NULL,
    @error_message NVARCHAR(2000) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @status NOT IN ('SUCCEEDED', 'FAILED')
        THROW 50001, 'Audit completion status must be SUCCEEDED or FAILED.', 1;

    UPDATE etl.pipeline_run_audit
    SET run_end_at = SYSUTCDATETIME(),
        rows_read = @rows_read,
        rows_written = @rows_written,
        status = @status,
        error_message =
            CASE
                WHEN @status = 'FAILED' THEN @error_message
                ELSE NULL
            END,
        updated_at = SYSUTCDATETIME()
    WHERE run_id = @run_id
      AND source_name = @source_name;

    IF @@ROWCOUNT = 0
        THROW 50002, 'No matching RUNNING audit record was found for this run/source.', 1;
END;
GO

CREATE OR ALTER VIEW etl.vw_pipeline_run_audit_recent
AS
SELECT
    audit_id,
    run_id,
    pipeline_name,
    source_name,
    activity_name,
    run_start_at,
    run_end_at,
    rows_read,
    rows_written,
    status,
    error_message,
    DATEDIFF(SECOND, run_start_at, run_end_at) AS duration_seconds,
    created_at,
    updated_at
FROM etl.pipeline_run_audit;
GO

PRINT 'Pipeline audit table, procedures, and view created successfully.';
GO

-- Verification:
-- SELECT TOP (100) *
-- FROM etl.vw_pipeline_run_audit_recent
-- ORDER BY run_start_at DESC, audit_id DESC;
