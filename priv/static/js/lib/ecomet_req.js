var ecomet;
if (!ecomet) {
    ecomet = new Ecomet();
}

if (typeof define == 'function') {
    define(ecomet);
}