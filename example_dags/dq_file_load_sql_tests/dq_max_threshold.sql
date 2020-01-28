SELECT 1.2*MAX(revenue) FROM Monthly_Return WHERE date>=(DATE('{{ ds }}') - INTERVAL '1 month');
