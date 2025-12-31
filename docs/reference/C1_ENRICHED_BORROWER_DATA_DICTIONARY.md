# Dicionário de dados — `C1_ENRICHED_BORROWER`

> Gerado automaticamente em: **2025-12-31T15:08:23-0300** (America/Sao_Paulo).

## Fonte

- View: `CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER`
- Colunas: **56**

## Campos (descrição + fill-rate)

_`fill_rate_*` = fração de linhas onde a coluna é **não-nula**. Valores são um snapshot do momento de geração._

| column                                  | snowflake_type | nullable | description                                                            | fill_rate_all        | credit_simulation     | pre_analysis        |
| --------------------------------------- | -------------- | -------- | ---------------------------------------------------------------------- | -------------------- | --------------------- | ------------------- |
| boa_vista_score                         | NUMBER         | YES      | Scores de bureau + fonte.                                              | 0.5005957473639809   | 0.2895135573537815    | 0.5173957655119761  |
| borrower_birthdate                      | TIMESTAMP_NTZ  | YES      | Cadastro/demografia do tomador (com *_source para linhagem).           | 0.9379101878234892   | 0.953715138891449     | 0.9366522727679505  |
| borrower_birthdate_source               | TEXT           | YES      | Cadastro/demografia do tomador (com *_source para linhagem).           | 0.9379101878234892   | 0.953715138891449     | 0.9366522727679505  |
| borrower_city                           | TEXT           | YES      | Cadastro/demografia do tomador (com *_source para linhagem).           | 0.11505968888507186  | 0.09790787266594164   | 0.11642480085277762 |
| borrower_gender                         | TEXT           | YES      | Cadastro/demografia do tomador (com *_source para linhagem).           | 0.17696659140101734  | 0.477716539787285     | 0.1530299220370423  |
| borrower_gender_source                  | TEXT           | YES      | Cadastro/demografia do tomador (com *_source para linhagem).           | 0.17696659140101734  | 0.477716539787285     | 0.1530299220370423  |
| borrower_state                          | TEXT           | YES      | Cadastro/demografia do tomador (com *_source para linhagem).           | 0.11501022445977781  | 0.0978673204180568    | 0.11637462710784743 |
| borrower_zipcode                        | TEXT           | YES      | Cadastro/demografia do tomador (com *_source para linhagem).           | 0.9991112710400466   | 0.9996718954489319    | 0.9990666509788135  |
| borrower_zipcode_source                 | TEXT           | YES      | Cadastro/demografia do tomador (com *_source para linhagem).           | 0.999164268638576    | 0.9999852537280419    | 0.9990989264872481  |
| c1_appealable                           | BOOLEAN        | YES      | Retry/appeal: canônico no CS; inferido no legado com prob/source.      | 0.18742315348213248  | 0.4278962599767746    | 0.16828391415066854 |
| c1_appealable_inference_source          | TEXT           | YES      | Retry/appeal: canônico no CS; inferido no legado com prob/source.      | 0.6502789032595425   | 1.0                   | 0.6224446232964033  |
| c1_appealable_prob                      | FLOAT          | YES      | Retry/appeal: canônico no CS; inferido no legado com prob/source.      | 0.5119054348901401   | 0.0                   | 0.5526479560654043  |
| c1_approved_amount                      | FLOAT          | YES      | Sinal/valor de aprovação (semântica definida no core).                 | 0.3701382177369645   | 0.3362739857329819    | 0.3728334698196122  |
| c1_can_retry_with_financial_responsible | BOOLEAN        | YES      | Campo do modelo C1 oficial (ver cores de enrichment).                  | 0.18742315348213248  | 0.4278962599767746    | 0.16828391415066854 |
| c1_created_at                           | TIMESTAMP_NTZ  | YES      | Chave/tempo da entidade no funil.                                      | 1.0                  | 1.0                   | 1.0                 |
| c1_entity_id                            | NUMBER         | YES      | Chave/tempo da entidade no funil.                                      | 1.0                  | 1.0                   | 1.0                 |
| c1_entity_type                          | TEXT           | NO       | Chave/tempo da entidade no funil.                                      | 1.0                  | 1.0                   | 1.0                 |
| c1_has_counter_proposal                 | BOOLEAN        | YES      | Contra-oferta (proxy no CS; canônico no legado).                       | 1.0                  | 1.0                   | 1.0                 |
| c1_outcome_bucket                       | TEXT           | YES      | Campo do modelo C1 oficial (ver cores de enrichment).                  | 1.0                  | 1.0                   | 1.0                 |
| c1_rejection_reason                     | TEXT           | YES      | Motivo de reprovação/recusa.                                           | 0.6098493454932473   | 0.45159720558146393   | 0.6224446232964033  |
| c1_requested_amount                     | FLOAT          | YES      | Valor solicitado/simulado.                                             | 0.9997553956990954   | 0.9966820888094229    | 1.0                 |
| c1_state_raw                            | TEXT           | YES      | Campo do modelo C1 oficial (ver cores de enrichment).                  | 1.0                  | 1.0                   | 1.0                 |
| c1_was_approved                         | BOOLEAN        | YES      | Sinal/valor de aprovação (semântica definida no core).                 | 0.9962246685068158   | 1.0                   | 0.995924190112134   |
| cadastro_evidence_source                | TEXT           | YES      | Linhagem/feature do eixo cadastro/demografia.                          | 0.999081374958825    | 0.9999852537280419    | 0.9990094353047703  |
| clinic_credit_score_at_c1               | FLOAT          | YES      | Risco/score dinâmico da clínica (join temporal via CLINIC_SCORE_LOGS). | 0.5706058468037828   | 0.6584947742898748    | 0.5636107726257916  |
| clinic_credit_score_changed_at_matched  | TIMESTAMP_NTZ  | YES      | Risco/score dinâmico da clínica (join temporal via CLINIC_SCORE_LOGS). | 0.5706058468037828   | 0.6584947742898748    | 0.5636107726257916  |
| clinic_credit_score_days_from_c1        | NUMBER         | YES      | Risco/score dinâmico da clínica (join temporal via CLINIC_SCORE_LOGS). | 0.5706058468037828   | 0.6584947742898748    | 0.5636107726257916  |
| clinic_credit_score_match_stage         | TEXT           | YES      | Risco/score dinâmico da clínica (join temporal via CLINIC_SCORE_LOGS). | 1.0                  | 1.0                   | 1.0                 |
| clinic_id                               | NUMBER         | YES      | Chave/tempo da entidade no funil.                                      | 1.0                  | 1.0                   | 1.0                 |
| financing_installment_value_max         | FLOAT          | YES      | Condições de financiamento (prazo/parcela/dívida total min/max).       | 0.3731253798161228   | 0.33625923946102376   | 0.3760595535945087  |
| financing_installment_value_min         | FLOAT          | YES      | Condições de financiamento (prazo/parcela/dívida total min/max).       | 0.3731253798161228   | 0.33625923946102376   | 0.3760595535945087  |
| financing_term_max                      | NUMBER         | YES      | Condições de financiamento (prazo/parcela/dívida total min/max).       | 0.37314657885553454  | 0.33625923946102376   | 0.376082439864126   |
| financing_term_min                      | NUMBER         | YES      | Condições de financiamento (prazo/parcela/dívida total min/max).       | 0.3731454917253083   | 0.33625923946102376   | 0.3760812662092738  |
| financing_total_debt_max                | FLOAT          | YES      | Condições de financiamento (prazo/parcela/dívida total min/max).       | 0.3731253798161228   | 0.33625923946102376   | 0.3760595535945087  |
| financing_total_debt_min                | FLOAT          | YES      | Condições de financiamento (prazo/parcela/dívida total min/max).       | 0.3731253798161228   | 0.33625923946102376   | 0.3760595535945087  |
| income_estimated                        | FLOAT          | YES      | Renda e proxies (inclui SCR) + fonte.                                  | 0.17939034824042538  | 0.21904481023391273   | 0.1762342521192539  |
| income_estimated_source                 | TEXT           | YES      | Renda e proxies (inclui SCR) + fonte.                                  | 0.17939034824042538  | 0.21904481023391273   | 0.1762342521192539  |
| negativacao_source                      | TEXT           | YES      | Negativação (contagens/valores) + fonte.                               | 0.7758908216856389   | 0.950349302317008     | 0.7620056822499668  |
| payment_default_risk                    | FLOAT          | YES      | Probabilidade/score contínuo (não é o risco 0..5/-1/9).                | 0.012038608342854782 | 0.16329652909623785   | 0.0                 |
| pefin_count                             | NUMBER         | YES      | Negativação (contagens/valores) + fonte.                               | 0.27205406733467197  | 0.948513391458222     | 0.21821471252644759 |
| pefin_value                             | FLOAT          | YES      | Negativação (contagens/valores) + fonte.                               | 0.4417368862198634   | 0.37412766584947743   | 0.4471178997915882  |
| protesto_count                          | FLOAT          | YES      | Negativação (contagens/valores) + fonte.                               | 0.2460923104017707   | 0.8607951927153417    | 0.1971681468899467  |
| protesto_value                          | FLOAT          | YES      | Negativação (contagens/valores) + fonte.                               | 0.2328766118064517   | 0.285170780262115     | 0.22871452224765468 |
| refin_count                             | NUMBER         | YES      | Negativação (contagens/valores) + fonte.                               | 0.2743297026807544   | 0.8611638495142947    | 0.2276236100625646  |
| refin_value                             | FLOAT          | YES      | Negativação (contagens/valores) + fonte.                               | 0.16567891826193967  | 0.2851744668301045    | 0.15616827511173928 |
| renda_proxies_source                    | TEXT           | YES      | Renda e proxies (inclui SCR) + fonte.                                  | 0.27924461843359755  | 0.7616191406610017    | 0.24085246072877511 |
| risk_capim                              | NUMBER         | YES      | Risco paciente (0..5, -1, 9) e versões safe-for-aggregation.           | 0.907412651804147    | 0.9993401043298741    | 0.9000961516737639  |
| risk_capim_0_5                          | NUMBER         | YES      | Risco paciente (0..5, -1, 9) e versões safe-for-aggregation.           | 0.4335401960965502   | 0.39306187904370427   | 0.4367618627897717  |
| risk_capim_is_special                   | BOOLEAN        | YES      | Risco paciente (0..5, -1, 9) e versões safe-for-aggregation.           | 1.0                  | 1.0                   | 1.0                 |
| risk_capim_raw                          | NUMBER         | YES      | Risco paciente (0..5, -1, 9) e versões safe-for-aggregation.           | 0.907412651804147    | 0.9993401043298741    | 0.9000961516737639  |
| risk_capim_special_kind                 | TEXT           | YES      | Risco paciente (0..5, -1, 9) e versões safe-for-aggregation.           | 0.4738724557075968   | 0.6062782252861698    | 0.46333428888399214 |
| risk_capim_subclass                     | TEXT           | YES      | Risco paciente (0..5, -1, 9) e versões safe-for-aggregation.           | 0.37207548380012895  | 0.29194300565888187   | 0.37845322266550513 |
| scr_operations_count                    | NUMBER         | YES      | Negativação (contagens/valores) + fonte.                               | 0.1309991922622419   | 0.0016515824593095058 | 0.14129396034278938 |
| serasa_score                            | NUMBER         | YES      | Scores de bureau + fonte.                                              | 0.4665938470603455   | 0.5181397577924831    | 0.46249131128642257 |
| serasa_score_source                     | TEXT           | YES      | Scores de bureau + fonte.                                              | 0.4665938470603455   | 0.5181397577924831    | 0.46249131128642257 |
| total_negative_value                    | FLOAT          | YES      | Negativação (contagens/valores) + fonte.                               | 0.46655715641520984  | 0.3741497852574146    | 0.47391185323915536 |
