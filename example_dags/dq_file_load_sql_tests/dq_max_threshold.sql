SELECT 1.2*MAX(revenue) FROM Monthly_Return WHERE date>=(NOW() - INTERVAL '1 month');
