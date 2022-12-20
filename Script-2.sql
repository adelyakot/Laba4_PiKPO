create table source_files (
	id integer PRIMARY KEY autoincrement,
	filename varchar (255) NOT NULL,
	processed datetime
	);
create table processed_data1 (
	id integer PRIMARY KEY autoincrement,
	genres varchar (5000) NOT NULL,
	homepage varchar (150) NULL,
	title_movie varchar (250) NOT NULL,
	production_countries varchar (255) NOT NULL,
	Release_year integer NOT NULL,
	Runtime integer NOT NULL,
	tagline varchar (200) NULL,
	source_file integer NOT NULL,
	CONSTRAINT fk_source_files
	FOREIGN KEY (source_file)
	REFERENCES source_files (id)
	ON DELETE CASCADE
);