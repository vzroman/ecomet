{define, 'MODULE_TEST', "../"}.

{suites, 'MODULE_TEST', [
    %ecomet_node_SUITE
    %ecomet_db_SUITE
    %ecomet_object_SUITE
    %ecomet_field_SUITE
    ecomet_pattern_SUITE

]}.

