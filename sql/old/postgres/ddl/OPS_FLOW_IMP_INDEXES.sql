create index OPTION_FLOW_IMP_INDEX1 on flow.ops_flow_imp(tick_symb) TABLESPACE indexspace;
create index OPTION_FLOW_IMP_INDEX2 on flow.ops_flow_imp(flow_imp_dt) TABLESPACE indexspace;
create index OPTION_FLOW_IMP_INDEX3 on flow.ops_flow_imp(dtls_side) TABLESPACE indexspace;
create index OPTION_FLOW_IMP_INDEX4 on flow.ops_flow_imp(opts_exp) TABLESPACE indexspace;
create index OPTION_FLOW_IMP_INDEX5 on flow.ops_flow_imp(opts_typ) TABLESPACE indexspace;