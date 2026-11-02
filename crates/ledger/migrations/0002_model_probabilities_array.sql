ALTER TABLE decision_ledger
    DROP CONSTRAINT decision_ledger_model_probabilities_check,
    DROP CONSTRAINT decision_ledger_model_probabilities_check1,
    DROP CONSTRAINT decision_ledger_model_probabilities_check2;

ALTER TABLE decision_ledger
    ADD CONSTRAINT decision_ledger_model_probabilities_is_array
        CHECK (jsonb_typeof(model_probabilities) = 'array'),
    ADD CONSTRAINT decision_ledger_model_probabilities_has_three_classes
        CHECK (jsonb_array_length(model_probabilities) = 3);