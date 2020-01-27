SELECT 0.8*MIN(revenue) FROM Monthly_Return WHERE date>=(NOW() - INTERVAL '1 month');
