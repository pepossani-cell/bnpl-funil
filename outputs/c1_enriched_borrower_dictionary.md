# Dicionário — C1_ENRICHED_BORROWER (oficial)

- View: `CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER`
- Colunas: **56**

## Dicionário de colunas

| column                                  | snowflake_type | nullable | description                                                            |
| --------------------------------------- | -------------- | -------- | ---------------------------------------------------------------------- |
| c1_entity_type                          | TEXT           | NO       | Chave/tempo da entidade no funil.                                      |
| c1_entity_id                            | NUMBER         | YES      | Chave/tempo da entidade no funil.                                      |
| c1_created_at                           | TIMESTAMP_NTZ  | YES      | Chave/tempo da entidade no funil.                                      |
| clinic_id                               | NUMBER         | YES      | Chave/tempo da entidade no funil.                                      |
| risk_capim                              | NUMBER         | YES      | Risco paciente (0..5, -1, 9) e versões safe-for-aggregation.           |
| risk_capim_subclass                     | TEXT           | YES      | Risco paciente (0..5, -1, 9) e versões safe-for-aggregation.           |
| payment_default_risk                    | FLOAT          | YES      | Probabilidade/score contínuo (não é o risco 0..5/-1/9).                |
| c1_state_raw                            | TEXT           | YES      | Campo do modelo C1 oficial (ver cores de enrichment).                  |
| c1_was_approved                         | BOOLEAN        | YES      | Sinal/valor de aprovação (semântica definida no core).                 |
| c1_outcome_bucket                       | TEXT           | YES      | Campo do modelo C1 oficial (ver cores de enrichment).                  |
| c1_rejection_reason                     | TEXT           | YES      | Motivo de reprovação/recusa.                                           |
| c1_can_retry_with_financial_responsible | BOOLEAN        | YES      | Campo do modelo C1 oficial (ver cores de enrichment).                  |
| c1_appealable                           | BOOLEAN        | YES      | Retry/appeal: canônico no CS; inferido no legado com prob/source.      |
| c1_appealable_prob                      | FLOAT          | YES      | Retry/appeal: canônico no CS; inferido no legado com prob/source.      |
| c1_appealable_inference_source          | TEXT           | YES      | Retry/appeal: canônico no CS; inferido no legado com prob/source.      |
| c1_requested_amount                     | FLOAT          | YES      | Valor solicitado/simulado.                                             |
| c1_approved_amount                      | FLOAT          | YES      | Sinal/valor de aprovação (semântica definida no core).                 |
| c1_has_counter_proposal                 | BOOLEAN        | YES      | Contra-oferta (proxy no CS; canônico no legado).                       |
| financing_term_min                      | NUMBER         | YES      | Condições de financiamento (prazo/parcela/dívida total min/max).       |
| financing_term_max                      | NUMBER         | YES      | Condições de financiamento (prazo/parcela/dívida total min/max).       |
| financing_installment_value_min         | FLOAT          | YES      | Condições de financiamento (prazo/parcela/dívida total min/max).       |
| financing_installment_value_max         | FLOAT          | YES      | Condições de financiamento (prazo/parcela/dívida total min/max).       |
| financing_total_debt_min                | FLOAT          | YES      | Condições de financiamento (prazo/parcela/dívida total min/max).       |
| financing_total_debt_max                | FLOAT          | YES      | Condições de financiamento (prazo/parcela/dívida total min/max).       |
| borrower_birthdate                      | TIMESTAMP_NTZ  | YES      | Cadastro/demografia do tomador (com *_source para linhagem).           |
| borrower_gender                         | TEXT           | YES      | Cadastro/demografia do tomador (com *_source para linhagem).           |
| borrower_city                           | TEXT           | YES      | Cadastro/demografia do tomador (com *_source para linhagem).           |
| borrower_state                          | TEXT           | YES      | Cadastro/demografia do tomador (com *_source para linhagem).           |
| borrower_zipcode                        | TEXT           | YES      | Cadastro/demografia do tomador (com *_source para linhagem).           |
| borrower_birthdate_source               | TEXT           | YES      | Cadastro/demografia do tomador (com *_source para linhagem).           |
| borrower_gender_source                  | TEXT           | YES      | Cadastro/demografia do tomador (com *_source para linhagem).           |
| borrower_zipcode_source                 | TEXT           | YES      | Cadastro/demografia do tomador (com *_source para linhagem).           |
| cadastro_evidence_source                | TEXT           | YES      | Linhagem/feature do eixo cadastro/demografia.                          |
| pefin_count                             | NUMBER         | YES      | Negativação (contagens/valores) + fonte.                               |
| refin_count                             | NUMBER         | YES      | Negativação (contagens/valores) + fonte.                               |
| protesto_count                          | FLOAT          | YES      | Negativação (contagens/valores) + fonte.                               |
| pefin_value                             | FLOAT          | YES      | Negativação (contagens/valores) + fonte.                               |
| refin_value                             | FLOAT          | YES      | Negativação (contagens/valores) + fonte.                               |
| protesto_value                          | FLOAT          | YES      | Negativação (contagens/valores) + fonte.                               |
| total_negative_value                    | FLOAT          | YES      | Negativação (contagens/valores) + fonte.                               |
| negativacao_source                      | TEXT           | YES      | Negativação (contagens/valores) + fonte.                               |
| income_estimated                        | FLOAT          | YES      | Renda e proxies (inclui SCR) + fonte.                                  |
| income_estimated_source                 | TEXT           | YES      | Renda e proxies (inclui SCR) + fonte.                                  |
| scr_operations_count                    | NUMBER         | YES      | Negativação (contagens/valores) + fonte.                               |
| renda_proxies_source                    | TEXT           | YES      | Renda e proxies (inclui SCR) + fonte.                                  |
| serasa_score                            | NUMBER         | YES      | Scores de bureau + fonte.                                              |
| serasa_score_source                     | TEXT           | YES      | Scores de bureau + fonte.                                              |
| boa_vista_score                         | NUMBER         | YES      | Scores de bureau + fonte.                                              |
| risk_capim_raw                          | NUMBER         | YES      | Risco paciente (0..5, -1, 9) e versões safe-for-aggregation.           |
| risk_capim_0_5                          | NUMBER         | YES      | Risco paciente (0..5, -1, 9) e versões safe-for-aggregation.           |
| risk_capim_is_special                   | BOOLEAN        | YES      | Risco paciente (0..5, -1, 9) e versões safe-for-aggregation.           |
| risk_capim_special_kind                 | TEXT           | YES      | Risco paciente (0..5, -1, 9) e versões safe-for-aggregation.           |
| clinic_credit_score_at_c1               | FLOAT          | YES      | Risco/score dinâmico da clínica (join temporal via CLINIC_SCORE_LOGS). |
| clinic_credit_score_changed_at_matched  | TIMESTAMP_NTZ  | YES      | Risco/score dinâmico da clínica (join temporal via CLINIC_SCORE_LOGS). |
| clinic_credit_score_days_from_c1        | NUMBER         | YES      | Risco/score dinâmico da clínica (join temporal via CLINIC_SCORE_LOGS). |
| clinic_credit_score_match_stage         | TEXT           | YES      | Risco/score dinâmico da clínica (join temporal via CLINIC_SCORE_LOGS). |

## Fill-rate (resumo)

### __all__

| column                                  | fill_rate            | n_nonnull | n_rows  |
| --------------------------------------- | -------------------- | --------- | ------- |
| payment_default_risk                    | 0.012038608342854782 | 44295     | 3679412 |
| borrower_state                          | 0.11501022445977781  | 423170    | 3679412 |
| borrower_city                           | 0.11505968888507186  | 423352    | 3679412 |
| scr_operations_count                    | 0.1309991922622419   | 482000    | 3679412 |
| refin_value                             | 0.16567891826193967  | 609601    | 3679412 |
| borrower_gender                         | 0.17696659140101734  | 651133    | 3679412 |
| borrower_gender_source                  | 0.17696659140101734  | 651133    | 3679412 |
| income_estimated                        | 0.17939034824042538  | 660051    | 3679412 |
| income_estimated_source                 | 0.17939034824042538  | 660051    | 3679412 |
| c1_appealable                           | 0.18742315348213248  | 689607    | 3679412 |
| c1_can_retry_with_financial_responsible | 0.18742315348213248  | 689607    | 3679412 |
| protesto_value                          | 0.2328766118064517   | 856849    | 3679412 |
| protesto_count                          | 0.2460923104017707   | 905475    | 3679412 |
| pefin_count                             | 0.27205406733467197  | 1000999   | 3679412 |
| refin_count                             | 0.2743297026807544   | 1009372   | 3679412 |

### credit_simulation

| column                          | fill_rate             | n_nonnull | n_rows |
| ------------------------------- | --------------------- | --------- | ------ |
| c1_appealable_prob              | 0.0                   | 0         | 271255 |
| scr_operations_count            | 0.0016515824593095058 | 448       | 271255 |
| borrower_state                  | 0.0978673204180568    | 26547     | 271255 |
| borrower_city                   | 0.09790787266594164   | 26558     | 271255 |
| payment_default_risk            | 0.16329652909623785   | 44295     | 271255 |
| income_estimated                | 0.21904481023391273   | 59417     | 271255 |
| income_estimated_source         | 0.21904481023391273   | 59417     | 271255 |
| protesto_value                  | 0.285170780262115     | 77354     | 271255 |
| refin_value                     | 0.2851744668301045    | 77355     | 271255 |
| boa_vista_score                 | 0.2895135573537815    | 78532     | 271255 |
| risk_capim_subclass             | 0.29194300565888187   | 79191     | 271255 |
| financing_installment_value_max | 0.33625923946102376   | 91212     | 271255 |
| financing_installment_value_min | 0.33625923946102376   | 91212     | 271255 |
| financing_term_max              | 0.33625923946102376   | 91212     | 271255 |
| financing_term_min              | 0.33625923946102376   | 91212     | 271255 |

### pre_analysis

| column                                  | fill_rate           | n_nonnull | n_rows  |
| --------------------------------------- | ------------------- | --------- | ------- |
| payment_default_risk                    | 0.0                 | 0         | 3408157 |
| borrower_state                          | 0.11637462710784743 | 396623    | 3408157 |
| borrower_city                           | 0.11642480085277762 | 396794    | 3408157 |
| scr_operations_count                    | 0.14129396034278938 | 481552    | 3408157 |
| borrower_gender                         | 0.1530299220370423  | 521550    | 3408157 |
| borrower_gender_source                  | 0.1530299220370423  | 521550    | 3408157 |
| refin_value                             | 0.15616827511173928 | 532246    | 3408157 |
| c1_appealable                           | 0.16828391415066854 | 573538    | 3408157 |
| c1_can_retry_with_financial_responsible | 0.16828391415066854 | 573538    | 3408157 |
| income_estimated                        | 0.1762342521192539  | 600634    | 3408157 |
| income_estimated_source                 | 0.1762342521192539  | 600634    | 3408157 |
| protesto_count                          | 0.1971681468899467  | 671980    | 3408157 |
| pefin_count                             | 0.21821471252644759 | 743710    | 3408157 |
| refin_count                             | 0.2276236100625646  | 775777    | 3408157 |
| protesto_value                          | 0.22871452224765468 | 779495    | 3408157 |
