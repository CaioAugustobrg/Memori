import { Migration } from '../base.js';

export const mysqlMigrations: Record<number, Migration[]> = {
  1: [
    {
      description: 'create table memori_schema_version',
      operation: `
        CREATE TABLE IF NOT EXISTS memori_schema_version(
            num BIGINT NOT NULL PRIMARY KEY
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `,
    },
    {
      description: 'create table memori_entity',
      operation: `
        CREATE TABLE IF NOT EXISTS memori_entity(
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            uuid VARCHAR(36) NOT NULL,
            external_id VARCHAR(100) NOT NULL,
            date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            date_updated DATETIME DEFAULT NULL,
            UNIQUE KEY uk_memori_entity_external_id (external_id),
            UNIQUE KEY uk_memori_entity_uuid (uuid)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `,
    },
    {
      description: 'create table memori_process',
      operation: `
        CREATE TABLE IF NOT EXISTS memori_process(
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            uuid VARCHAR(36) NOT NULL,
            external_id VARCHAR(100) NOT NULL,
            date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            date_updated DATETIME DEFAULT NULL,
            UNIQUE KEY uk_memori_process_external_id (external_id),
            UNIQUE KEY uk_memori_process_uuid (uuid)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `,
    },
    {
      description: 'create table memori_session',
      operation: `
        CREATE TABLE IF NOT EXISTS memori_session(
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            uuid VARCHAR(36) NOT NULL,
            entity_id BIGINT DEFAULT NULL,
            process_id BIGINT DEFAULT NULL,
            date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            date_updated DATETIME DEFAULT NULL,
            UNIQUE KEY uk_memori_session_entity_id (entity_id, id),
            UNIQUE KEY uk_memori_session_process_id (process_id, id),
            UNIQUE KEY uk_memori_session_uuid (uuid),
            CONSTRAINT fk_memori_sess_entity FOREIGN KEY (entity_id) REFERENCES memori_entity (id) ON DELETE CASCADE,
            CONSTRAINT fk_memori_sess_process FOREIGN KEY (process_id) REFERENCES memori_process (id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `,
    },
    {
      description: 'create table memori_conversation',
      operation: `
        CREATE TABLE IF NOT EXISTS memori_conversation(
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            uuid VARCHAR(36) NOT NULL,
            session_id BIGINT NOT NULL,
            summary TEXT DEFAULT NULL,
            date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            date_updated DATETIME DEFAULT NULL,
            UNIQUE KEY uk_memori_conversation_session_id (session_id),
            UNIQUE KEY uk_memori_conversation_uuid (uuid),
            CONSTRAINT fk_memori_conv_session FOREIGN KEY (session_id) REFERENCES memori_session (id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `,
    },
    {
      description: 'create table memori_conversation_message',
      operation: `
        CREATE TABLE IF NOT EXISTS memori_conversation_message(
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            uuid VARCHAR(36) NOT NULL,
            conversation_id BIGINT NOT NULL,
            role VARCHAR(255) NOT NULL,
            type VARCHAR(255) DEFAULT NULL,
            content LONGTEXT NOT NULL,
            date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            date_updated DATETIME DEFAULT NULL,
            UNIQUE KEY uk_memori_conversation_message_conv_id (conversation_id, id),
            UNIQUE KEY uk_memori_conversation_message_uuid (uuid),
            CONSTRAINT fk_memori_conv_msg_conv FOREIGN KEY (conversation_id) REFERENCES memori_conversation (id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `,
    },
    {
      description: 'create table memori_entity_fact',
      operation: `
        CREATE TABLE IF NOT EXISTS memori_entity_fact(
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            uuid VARCHAR(36) NOT NULL,
            entity_id BIGINT NOT NULL,
            content LONGTEXT NOT NULL,
            content_embedding LONGBLOB NOT NULL,
            num_times BIGINT NOT NULL,
            date_last_time DATETIME NOT NULL,
            uniq CHAR(64) NOT NULL,
            date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            date_updated DATETIME DEFAULT NULL,
            UNIQUE KEY uk_memori_entity_fact_entity_id (entity_id, id),
            UNIQUE KEY uk_memori_entity_fact_entity_id_uniq (entity_id, uniq),
            UNIQUE KEY uk_memori_entity_fact_uuid (uuid),
            CONSTRAINT fk_memori_ent_fact_entity FOREIGN KEY (entity_id) REFERENCES memori_entity (id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `,
    },
    {
      description: 'create indexes for memori_entity_fact',
      operation: `
        CREATE INDEX idx_memori_entity_fact_entity_id_freq 
        ON memori_entity_fact (entity_id, num_times DESC, date_last_time DESC);
      `,
    },
    {
      description: 'create table memori_process_attribute',
      operation: `
        CREATE TABLE IF NOT EXISTS memori_process_attribute(
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            uuid VARCHAR(36) NOT NULL,
            process_id BIGINT NOT NULL,
            content LONGTEXT NOT NULL,
            num_times BIGINT NOT NULL,
            date_last_time DATETIME NOT NULL,
            uniq CHAR(64) NOT NULL,
            date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            date_updated DATETIME DEFAULT NULL,
            UNIQUE KEY uk_memori_process_attr_process_id (process_id, id),
            UNIQUE KEY uk_memori_process_attr_process_uniq (process_id, uniq),
            UNIQUE KEY uk_memori_process_attr_uuid (uuid),
            CONSTRAINT fk_memori_proc_attribute FOREIGN KEY (process_id) REFERENCES memori_process (id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `,
    },
    {
      description: 'create table memori_subject',
      operation: `
        CREATE TABLE IF NOT EXISTS memori_subject(
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            uuid VARCHAR(36) NOT NULL,
            name VARCHAR(255) NOT NULL,
            type VARCHAR(255) NOT NULL,
            uniq CHAR(64) NOT NULL,
            date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            date_updated DATETIME DEFAULT NULL,
            UNIQUE KEY uk_memori_subject_uniq (uniq),
            UNIQUE KEY uk_memori_subject_uuid (uuid)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `,
    },
    {
      description: 'create table memori_predicate',
      operation: `
        CREATE TABLE IF NOT EXISTS memori_predicate(
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            uuid VARCHAR(36) NOT NULL,
            content LONGTEXT NOT NULL,
            uniq CHAR(64) NOT NULL,
            date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            date_updated DATETIME DEFAULT NULL,
            UNIQUE KEY uk_memori_predicate_uniq (uniq),
            UNIQUE KEY uk_memori_predicate_uuid (uuid)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `,
    },
    {
      description: 'create table memori_object',
      operation: `
        CREATE TABLE IF NOT EXISTS memori_object(
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            uuid VARCHAR(36) NOT NULL,
            name VARCHAR(255) NOT NULL,
            type VARCHAR(255) NOT NULL,
            uniq CHAR(64) NOT NULL,
            date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            date_updated DATETIME DEFAULT NULL,
            UNIQUE KEY uk_memori_object_uniq (uniq),
            UNIQUE KEY uk_memori_object_uuid (uuid)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `,
    },
    {
      description: 'create table memori_knowledge_graph',
      operation: `
        CREATE TABLE IF NOT EXISTS memori_knowledge_graph(
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            uuid VARCHAR(36) NOT NULL,
            entity_id BIGINT NOT NULL,
            subject_id BIGINT NOT NULL,
            predicate_id BIGINT NOT NULL,
            object_id BIGINT NOT NULL,
            num_times BIGINT NOT NULL,
            date_last_time DATETIME NOT NULL,
            date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            date_updated DATETIME DEFAULT NULL,
            UNIQUE KEY uk_memori_kg_entity_id (entity_id, id),
            UNIQUE KEY uk_memori_kg_unique_triple (entity_id, subject_id, predicate_id, object_id),
            UNIQUE KEY uk_memori_kg_uuid (uuid),
            CONSTRAINT fk_memori_kg_entity FOREIGN KEY (entity_id) REFERENCES memori_entity (id) ON DELETE CASCADE,
            CONSTRAINT fk_memori_kg_object FOREIGN KEY (object_id) REFERENCES memori_object (id) ON DELETE CASCADE,
            CONSTRAINT fk_memori_kg_predicate FOREIGN KEY (predicate_id) REFERENCES memori_predicate (id) ON DELETE CASCADE,
            CONSTRAINT fk_memori_kg_subject FOREIGN KEY (subject_id) REFERENCES memori_subject (id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `,
    },
  ],
  2: [
    {
      description: 'create table memori_entity_fact_mention',
      operation: `
        CREATE TABLE IF NOT EXISTS memori_entity_fact_mention(
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            uuid VARCHAR(36) NOT NULL,
            entity_id BIGINT NOT NULL,
            fact_id BIGINT NOT NULL,
            conversation_id BIGINT NOT NULL,
            date_created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            date_updated DATETIME DEFAULT NULL,
            UNIQUE KEY uk_memori_ef_mention_uuid (uuid),
            UNIQUE KEY uk_memori_ef_mention_unique (entity_id, fact_id, conversation_id),
            CONSTRAINT fk_memori_ef_mention_ef FOREIGN KEY (entity_id, fact_id) REFERENCES memori_entity_fact (entity_id, id) ON DELETE CASCADE,
            CONSTRAINT fk_memori_ef_mention_conv FOREIGN KEY (conversation_id) REFERENCES memori_conversation (id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `,
    },
    {
      description: 'create indexes for memori_entity_fact_mention',
      operation: `
        CREATE INDEX idx_memori_ef_mention_ent_conv 
        ON memori_entity_fact_mention (entity_id, conversation_id);
      `,
    },
  ],
};