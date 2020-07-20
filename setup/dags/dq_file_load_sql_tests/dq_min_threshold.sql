SELECT 0.8*MIN(revenue) FROM Monthly_Return WHERE date>=(DATE('{{ ds }}') - INTERVAL '1 month');
