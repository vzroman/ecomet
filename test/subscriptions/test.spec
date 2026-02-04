{define, 'SUBSCRIPTIONS_TEST', "./."}.

{suites, 'SUBSCRIPTIONS_TEST', [
    %ecomet_subscription_pool_SUITE
    %,ecomet_subscription_query_SUITE
    ecomet_subscription_object_SUITE
]}.

