{define, 'MODULE_TEST', "./."}.

{suites, 'MODULE_TEST', [
    ecomet_bitmap_SUITE
    ,ecomet_backend_SUITE
    ,ecomet_bits_SUITE
    ,ecomet_field_SUITE
    ,ecomet_index_SUITE
    ,ecomet_object_SUITE
    ,ecomet_transaction_SUITE
    ,ecomet_resultset_SUITE
    ,ecomet_query_SUITE
]}.

