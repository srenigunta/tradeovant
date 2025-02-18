create table refdata.ref_attributes
(
	attributeid numeric not null,
	attributename varchar(200) not null,
	attributecategory varchar(100) not null
)
tablespace dataspaces;

commit;