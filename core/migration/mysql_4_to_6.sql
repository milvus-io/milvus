alter table Tables add column owner_table VARCHAR(255) DEFAULT '' NOT NULL;
alter table Tables add column partition_tag VARCHAR(255) DEFAULT '' NOT NULL;
alter table Tables add column version VARCHAR(64) DEFAULT '0.6.0' NOT NULL;
update Tables set version='0.6.0';
