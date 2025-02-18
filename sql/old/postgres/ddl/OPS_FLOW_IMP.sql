CREATE TABLE flow.ops_flow_imp (
	flow_imp_dt varchar(100) NULL,
	exec_tm varchar(100) NULL,
	tick_symb varchar(255) NULL,
	opts_exp varchar(100) NULL,
	opts_strk varchar(100) NULL,
	opts_typ varchar(100) NULL,
	tick_spot varchar(100) NULL,
	trans_dtls varchar(100) NULL,
	trans_typ varchar(100) NULL,
	opts_prem varchar(100) NULL,
	opts_price varchar(100) NULL,
	opts_iv varchar(100) NULL,
	opts_color varchar(100) NULL,
	opts_vol numeric NULL,
	days_to_exp numeric NULL,
	er_date varchar(100) NULL,
	div_date varchar(100) NULL,
	tick_indstry varchar(255) NULL,
	etf_flg varchar(100) NULL,
	tick_name varchar(255) NULL,
	dtls_side varchar(255) NULL,
	alrt_flg varchar(100) NULL,
	opts_oi numeric NULL,
	flow_skey numeric NULL DEFAULT nextval('flow.flow_seq'::regclass)
)
TABLESPACE dataspaces;

CREATE INDEX option_flow_imp_index1 ON flow.ops_flow_imp USING btree (tick_symb) tablespace indexspace;
CREATE INDEX option_flow_imp_index2 ON flow.ops_flow_imp USING btree (flow_imp_dt) tablespace indexspace;
CREATE INDEX option_flow_imp_index3 ON flow.ops_flow_imp USING btree (dtls_side) tablespace indexspace;
CREATE INDEX option_flow_imp_index4 ON flow.ops_flow_imp USING btree (opts_exp) tablespace indexspace;
CREATE INDEX option_flow_imp_index5 ON flow.ops_flow_imp USING btree (opts_typ) tablespace indexspace;
   
create table flow.dp_flow_imp
(
	dp_imp_dt varchar(50) not null,
	exec_dt varchar(50),
	exec_tm varchar(50),
	tick_symb varchar(20) not null,
	dp_vol varchar(50),
	dp_price varchar(50),
	avg_30d_vol_perc varchar(20),
	notional varchar(50),
	dp_type varchar(50),
	sec_type varchar(50),
	industry varchar(500),
	sector varchar(500),
	avg_30d_vol varchar(50),
	sec_float varchar(50),
	earnings varchar(50),
	dp_cancel_flg varchar(3),
	dp_skey numeric NULL DEFAULT nextval('flow.dp_seq'::regclass)
)
tablespace dataspaces;

create index dp_flow_imp_indx1 on flow.dp_flow_imp(tick_symb)
tablespace indexspace;

create index dp_flow_imp_indx2 on flow.dp_flow_imp(dp_imp_dt)
tablespace indexspace;

create index dp_flow_imp_indx3 on flow.dp_flow_imp(dp_cancel_flg)
tablespace indexspace;
