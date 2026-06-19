-- Performance + GDPR knobs applied after first write.
ALTER TABLE delta.`s3://softgames-payments/silver/payments`
    SET TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'delta.bloomFilter.transaction_id' = 'true'
    );
