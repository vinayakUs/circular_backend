-- properties table

 CREATE TABLE properties (
    id          RAW(16)                  DEFAULT SYS_GUID() NOT NULL,
    name        VARCHAR2(255)            NOT NULL,
    type        VARCHAR2(50)             NOT NULL,
    archived    NUMBER(1)                DEFAULT 0 NOT NULL,
    archived_at TIMESTAMP WITH TIME ZONE,
    metadata    JSON                     DEFAULT '{}',
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_properties PRIMARY KEY (id),
    CONSTRAINT chk_properties_archived CHECK (archived IN (0, 1))
);
CREATE INDEX idx_properties_type 
    ON properties(type);


-- checkpoints table 

 CREATE TABLE scraper_checkpoints (
      source VARCHAR2(20) PRIMARY KEY,
      last_run_date DATE NOT NULL,
      updated_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
      es_bloom_filter BLOB,
      es_last_run_at TIMESTAMP WITH TIME ZONE,
      es_records_processed NUMBER(10) DEFAULT 0
  );

-- circular_assets

CREATE TABLE circular_assets (
    id                   RAW(16)                  DEFAULT SYS_GUID() NOT NULL,
      circular_id RAW(16) NOT NULL,
      asset_role VARCHAR2(30) NOT NULL,
      file_path VARCHAR2(500) NOT NULL,
      content_hash VARCHAR2(64),
      mime_type VARCHAR2(100),
      archive_member_path VARCHAR2(1000),
      file_size_bytes NUMBER(19),
      created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL ,
      updated_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
      
      
          CONSTRAINT pk_circular_assets PRIMARY KEY (id),
    CONSTRAINT fk_circular_assets_circular
        FOREIGN KEY (circular_id) REFERENCES circulars(id) ON DELETE CASCADE

  );
  
  CREATE INDEX idx_circular_assets_circular_id
    ON circular_assets(circular_id);
CREATE INDEX idx_circular_assets_circular_role
    ON circular_assets(circular_id, asset_role);
CREATE UNIQUE INDEX idx_circular_assets_identity
    ON circular_assets(circular_id, asset_role, NVL(archive_member_path, 'NO_ARCHIVE'));

-- circulars

CREATE TABLE circulars (
    id             RAW(16)        DEFAULT SYS_GUID() NOT NULL,
    circular_id    VARCHAR2(50)   NOT NULL,
    source         VARCHAR2(20)   NOT NULL,
    source_item_key VARCHAR2(4000),
    full_reference VARCHAR2(4000)           NOT NULL,
    department     VARCHAR2(50),
    title          VARCHAR2(4000)           NOT NULL,
    issue_date     DATE           NOT NULL,
    effective_date DATE,
    url            VARCHAR2(4000),
    pdf_url        VARCHAR2(4000),
    content_hash   VARCHAR2(64),
    status         VARCHAR2(20)   DEFAULT 'DISCOVERED' NOT NULL,
    error_message  CLOB,
    detected_at    TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at     TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at     TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    es_indexed_at  TIMESTAMP WITH TIME ZONE,
    es_chunk_count NUMBER(10),
    es_index_name  VARCHAR2(100),
    APPLICABLE_TO_NSE NUMBER(1,0) DEFAULT 0 NOT NULL
    CONSTRAINT pk_circulars PRIMARY KEY (id),
    CONSTRAINT uq_circulars_source UNIQUE (source, source_item_key)
);
CREATE INDEX idx_circulars_status ON circulars(status);
CREATE INDEX idx_circulars_source ON circulars(source);
CREATE INDEX idx_circulars_issue_date ON circulars(issue_date DESC);


-- processign task 


CREATE TABLE processing_tasks (
    id              RAW(16)                  DEFAULT SYS_GUID() NOT NULL,
    circular_id     RAW(16)                  NOT NULL,
    processor_name  VARCHAR2(50)             NOT NULL,
    status          VARCHAR2(20)             DEFAULT 'PENDING' NOT NULL,
    error_message   CLOB,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_processing_tasks 
        PRIMARY KEY (id),
    CONSTRAINT fk_processing_tasks_circular 
        FOREIGN KEY (circular_id) REFERENCES circulars(id) ON DELETE CASCADE,
    CONSTRAINT uq_processing_tasks_circular_processor 
        UNIQUE (circular_id, processor_name)
);

CREATE INDEX idx_processing_tasks_circular_id 
    ON processing_tasks(circular_id);


-- 



CREATE TABLE circular_department_mapping (
    id              RAW(16)                  DEFAULT SYS_GUID() NOT NULL,
    circular_id     RAW(16)                  NOT NULL,
    department_id   RAW(16)                  NOT NULL,
    expert_name     VARCHAR2(255)            NOT NULL,
    highlight_text  VARCHAR2(4000)           NOT NULL,
    highlights      JSON                     DEFAULT '[]',
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_circular_department_mapping 
        PRIMARY KEY (id),
    CONSTRAINT fk_cdm_circular 
        FOREIGN KEY (circular_id) REFERENCES circulars(id) ON DELETE CASCADE,
    CONSTRAINT fk_cdm_department 
        FOREIGN KEY (department_id) REFERENCES properties(id) ON DELETE CASCADE,
    CONSTRAINT uq_cdm_circular_dept_expert 
        UNIQUE (circular_id, department_id, expert_name)
);
CREATE INDEX idx_cdm_circular_id
    ON circular_department_mapping(circular_id);
CREATE INDEX idx_cdm_department_id
    ON circular_department_mapping(department_id);

-- circular_signatories

CREATE TABLE circular_signatories (
    id                       RAW(16)                  DEFAULT SYS_GUID() NOT NULL,
    circular_id              RAW(16)                  NOT NULL,
    signatory_name           VARCHAR2(500)            NOT NULL,
    signatory_designation    VARCHAR2(500)            NOT NULL,
    extracted_at            TIMESTAMP WITH TIME ZONE  DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_circular_signatories PRIMARY KEY (id),
    CONSTRAINT fk_circular_signatories_circular
        FOREIGN KEY (circular_id) REFERENCES circulars(id) ON DELETE CASCADE,
    CONSTRAINT uq_circular_signatories_identity
        UNIQUE (circular_id, signatory_name, signatory_designation)
);
CREATE INDEX idx_circular_signatories_circular_id
    ON circular_signatories(circular_id);

-- notification_logs

CREATE TABLE notification_logs (
    id              RAW(16) DEFAULT SYS_GUID() NOT NULL,
    template_name   VARCHAR2(100) NOT NULL,
    recipient_email VARCHAR2(255) NOT NULL,
    subject         VARCHAR2(500) NOT NULL,
    variables       JSON DEFAULT '{}',
    status          VARCHAR2(20) NOT NULL,
    error_message   CLOB,
    sent_at         TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_notification_logs PRIMARY KEY (id)
);
CREATE INDEX idx_notification_logs_status ON notification_logs(status);
CREATE INDEX idx_notification_logs_recipient ON notification_logs(recipient_email);
