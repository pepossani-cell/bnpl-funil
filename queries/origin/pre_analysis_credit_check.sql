{{ config(sort='pre_analysis_created_at', dist='pre_analysis_id') }}

with
    pre_analysis_sensitive as (

        select
            pi.pre_analysis_id,
            pi.pre_analysis_created_at,
            'pre_analysis' as pre_analysis_type,
            sha2(regexp_replace(pi.cpf, '[^0-9]', ''), 256) as hash_cpf,
            cpf_seventh_and_eighth_number
        from {{ ref('source_pre_analysis_api') }} pi

    ),
    credit_simulations_sensitive as (

        select
            cs.pre_analysis_id,
            cs.pre_analysis_created_at,
            'credit_simulation' as pre_analysis_type,
            sha2(regexp_replace(cs.cpf, '[^0-9]', ''), 256) as hash_cpf,
            cpf_seventh_and_eighth_number
        from {{ ref('source_restricted_credit_simulations') }} cs
        qualify row_number() over (partition by pre_analysis_id order by cs.pre_analysis_updated_at desc nulls last) = 1

    ),
    complete_pre_analysis as (
        select * from pre_analysis_sensitive
        union all
        select * from credit_simulations_sensitive
    ),
    serasa_prep_old_logic as (
        select
            pc.pre_analysis_id,
            pc.pre_analysis_created_at,
            pc.hash_cpf,
            pc.pre_analysis_type,
            si.serasa_consulted_at,
            si.serasa_ir_status,
            si.serasa_ccf,
            si.serasa_positive_score,
            si.serasa_refin,
            si.serasa_pefin,
            si.serasa_protest,
            si.serasa_presumed_income,
            si.has_error,
            abs(
                datediff(
                    'days',
                    pc.pre_analysis_created_at,
                    dateadd('hours', -3, si.serasa_consulted_at)
                )
            ) as days_from_consultation,
            row_number() over (
                partition by pc.pre_analysis_id order by days_from_consultation
            ) as credit_check_order
        from complete_pre_analysis as pc
        left join
            {{ ref('source_credit_checks_api_serasa') }} as si
            on si.hash_cpf = pc.hash_cpf
        where
            date(pc.pre_analysis_created_at) < '2024-04-04'
            and (si.kind is null or si.kind = 'check_score')
        qualify credit_check_order = 1
    ),
    aux_serasa_prep_new_logic as (
        select
            pc.pre_analysis_id,
            pc.pre_analysis_created_at,
            pc.hash_cpf,
            pc.pre_analysis_type,
            si.serasa_consulted_at,
            si.serasa_ir_status,
            si.serasa_ccf,
            si.serasa_positive_score,
            si.serasa_refin,
            si.serasa_pefin,
            si.serasa_protest,
            si.serasa_presumed_income,
            si.has_error,
            abs(
                datediff(
                    'days',
                    pc.pre_analysis_created_at,
                    dateadd('hours', -3, si.serasa_consulted_at)
                )
            ) as days_from_consultation,
            row_number() over (
                partition by pc.pre_analysis_id order by days_from_consultation
            ) as credit_check_order
        from complete_pre_analysis pc
        left join
           {{ ref('source_credit_checks_api_serasa') }} as si
            on si.hash_cpf = pc.hash_cpf
        where
            date(pc.pre_analysis_created_at) >= '2024-04-04'
            and si.kind in ('check_income_only', 'check_score_without_income')
        qualify credit_check_order < 3
    ),
    serasa_prep_new_logic as (

        select
            pc.pre_analysis_id,
            pc.pre_analysis_created_at,
            pc.hash_cpf,
            pc.pre_analysis_type,
            max(si.serasa_consulted_at) as serasa_consulted_at,
            max(si.serasa_ir_status) as serasa_ir_status,
            max(si.serasa_ccf) as serasa_ccf,
            max(si.serasa_positive_score) as serasa_positive_score,
            max(si.serasa_refin) as serasa_refin,
            max(si.serasa_pefin) as serasa_pefin,
            max(si.serasa_protest) as serasa_protest,
            max(si.serasa_presumed_income) as serasa_presumed_income,
            max(si.has_error) as has_error,
            min(si.days_from_consultation) as days_from_consultation,
            min(credit_check_order) as credit_check_order
        from complete_pre_analysis as pc
        left join
            aux_serasa_prep_new_logic si on si.pre_analysis_id = pc.pre_analysis_id
            and si.pre_analysis_type = pc.pre_analysis_type
        group by 1, 2, 3, 4

    ),
    serasa_c_c as (

        select
            cp.pre_analysis_id,
            cp.pre_analysis_created_at,
            cp.hash_cpf,
            cp.pre_analysis_type,
            coalesce(spo.has_error, spn.has_error) as has_error,
            case
                when
                    coalesce(spo.days_from_consultation, spn.days_from_consultation)
                    between 0 and 15
                then coalesce(spo.serasa_consulted_at, spn.serasa_consulted_at)
                else null
            end as serasa_consulted_at,
            case
                when
                    coalesce(spo.days_from_consultation, spn.days_from_consultation)
                    between 0 and 15
                then coalesce(spo.serasa_ir_status, spn.serasa_ir_status)
                else null
            end as serasa_ir_status,
            case
                when
                    coalesce(spo.days_from_consultation, spn.days_from_consultation)
                    between 0 and 15
                then coalesce(spo.serasa_ccf, spn.serasa_ccf)
                else null
            end as serasa_ccf,
            case
                when
                    coalesce(spo.days_from_consultation, spn.days_from_consultation)
                    between 0 and 15
                then coalesce(spo.serasa_positive_score, spn.serasa_positive_score)
                else null
            end as serasa_positive_score,
            case
                when
                    coalesce(spo.days_from_consultation, spn.days_from_consultation)
                    between 0 and 15
                then coalesce(spo.serasa_refin, spn.serasa_refin)
                else null
            end as serasa_refin,
            case
                when
                    coalesce(spo.days_from_consultation, spn.days_from_consultation)
                    between 0 and 15
                then coalesce(spo.serasa_pefin, spn.serasa_pefin)
                else null
            end as serasa_pefin,
            case
                when
                    coalesce(spo.days_from_consultation, spn.days_from_consultation)
                    between 0 and 15
                then coalesce(spo.serasa_protest, spn.serasa_protest)
                else null
            end as serasa_protest,
            case
                when
                    coalesce(spo.days_from_consultation, spn.days_from_consultation)
                    between 0 and 15
                then coalesce(spo.serasa_presumed_income, spn.serasa_presumed_income)
                else null
            end as serasa_presumed_income,
            coalesce(
                spo.days_from_consultation, spn.days_from_consultation
            ) as days_from_consultation
        from complete_pre_analysis as cp
        left join
            serasa_prep_old_logic as spo on spo.pre_analysis_id = cp.pre_analysis_id
            and spo.pre_analysis_type = cp.pre_analysis_type
        left join
            serasa_prep_new_logic as spn on spn.pre_analysis_id = cp.pre_analysis_id
            and spn.pre_analysis_type = cp.pre_analysis_type

    ),
    bvs_score_pf_prep as (

        select
            pc.pre_analysis_id,
            pc.pre_analysis_created_at,
            pc.hash_cpf,
            pc.pre_analysis_type,
            bvs.has_error,
            bvs.bvs_score_pf_net_consulted_at,
            bvs.bvs_positive_score,
            abs(
                datediff(
                    'days',
                    pc.pre_analysis_created_at,
                    dateadd('hours', -3, bvs.bvs_score_pf_net_consulted_at)
                )
            ) as days_from_consultation,
            row_number() over (
                partition by pc.pre_analysis_id order by days_from_consultation
            ) as credit_check_order
        from complete_pre_analysis as pc
        left join
            {{ ref('source_credit_checks_api_boa_vista_score_pf') }} as bvs
            on bvs.hash_cpf = pc.hash_cpf

    ),
    bvs_score_pf_c_c as (

        select
            pre_analysis_id,
            pre_analysis_created_at,
            hash_cpf,
            pre_analysis_type,
            has_error,
            case
                when days_from_consultation between 0 and 15
                then bvs_score_pf_net_consulted_at
                else null
            end as bvs_score_pf_net_consulted_at,
            case
                when days_from_consultation between 0 and 15
                then bvs_positive_score
                else null
            end as bvs_positive_score,
            days_from_consultation,
            credit_check_order
        from bvs_score_pf_prep
        where credit_check_order = 1

    ),
    bvs_scpc_prep as (

        select
            pc.pre_analysis_id,
            pc.pre_analysis_created_at,
            pc.hash_cpf,
            pc.pre_analysis_type,
            scpc.has_error,
            scpc.bvs_scpc_net_consulted_at,
            scpc.bvs_ccf_count,
            scpc.bvs_total_debt,
            scpc.bvs_total_protest,
            scpc.bvs_status_ir,
            abs(
                datediff(
                    'days',
                    pc.pre_analysis_created_at,
                    dateadd('hours', -3, scpc.bvs_scpc_net_consulted_at)
                )
            ) as days_from_consultation,
            row_number() over (
                partition by pc.pre_analysis_id order by days_from_consultation
            ) as credit_check_order
        from complete_pre_analysis as pc
        left join
            {{ ref('source_credit_checks_api_boa_vista_scpc_net') }} as scpc
            on pc.hash_cpf = scpc.hash_cpf

    ),
    bvs_scpc_c_c as (

        select
            pre_analysis_id,
            pre_analysis_created_at,
            hash_cpf,
            pre_analysis_type,
            has_error,
            case
                when days_from_consultation between 0 and 15
                then bvs_scpc_net_consulted_at
                else null
            end as bvs_scpc_net_consulted_at,
            case
                when days_from_consultation between 0 and 15
                then bvs_ccf_count
                else null
            end as bvs_ccf_count,
            case
                when days_from_consultation between 0 and 15
                then bvs_total_debt
                else null
            end as bvs_total_debt,
            case
                when days_from_consultation between 0 and 15
                then bvs_total_protest
                else null
            end as bvs_total_protest,
            case
                when days_from_consultation between 0 and 15
                then bvs_status_ir
                else null
            end as bvs_status_ir,
            days_from_consultation,
            credit_check_order
        from bvs_scpc_prep
        where credit_check_order = 1

    ),
    scr_report_prep as (

        select
            pc.pre_analysis_id,
            pc.pre_analysis_created_at,
            pc.hash_cpf,
            pc.pre_analysis_type,
            scr.has_error,
            scr.scr_report_consulted_at,
            scr.scr_mensagem_operador,
            scr.scr_data_inicio_relacionamento,
            scr.scr_prejuizo,
            scr.scr_repasses,
            scr.scr_risco_total,
            scr.scr_coobrigacoes,
            scr.scr_qtd_de_operacoes,
            scr.scr_carteira_vencer,
            scr.scr_carteira_vencido,
            scr.scr_creditos_a_liberar,
            scr.scr_limites_de_credito,
            scr.scr_qtd_de_instituicoes,
            scr.scr_vlr_operacoes_sob_judice,
            scr.scr_qtd_operacoes_sob_judice,
            scr.scr_carteira_a_vencer_31_a_60_dias,
            scr.scr_carteira_a_vencer_61_a_90_dias,
            scr.scr_carteira_a_vencer_91_a_180_dias,
            scr.scr_carteira_vencido_15_a_30_dias,
            scr.scr_carteira_vencido_31_a_60_dias,
            scr.scr_carteira_vencido_61_a_90_dias,
            scr.scr_vlr_operacoes_discordancia,
            scr.scr_carteira_a_vencer_181_a_360_dias,
            scr.scr_carteira_vencido_91_a_180_dias,
            scr.scr_perc_documentos_processados,
            scr.scr_qtd_de_operacoes_discordancia,
            scr.scr_carteira_a_vencer_acima_360_dias,
            scr.scr_carteira_vencido_181_a_360_dias,
            scr.scr_limites_de_credito_ate_360_dias,
            scr.scr_carteira_vencido_acima_360_dias,
            scr.scr_limites_de_credito_acima_360_dias,
            scr.scr_carteira_a_vencer_prazo_indeterminado,
            scr.scr_carteira_a_vencer_ate_30_dias_vencidos_ate_14_dias,
            scr.scr_a_vencer_adiantamentos_a_depositantes,
            scr.scr_a_vencer_cheque_especial_e_conta_garantida,
            scr.scr_a_vencer_credito_pessoal_consignado,
            scr.scr_a_vencer_credito_pessoal_sem_consignado,
            scr.scr_a_vencer_credito_rotativo_cartao_de_credito,
            scr.scr_a_vencer_cartao_de_credito_compra_fatura_ou_saque_financiado,
            scr.scr_a_vencer_home_equity,
            scr.scr_a_vencer_microcredito,
            scr.scr_a_vencer_cheque_especial,
            scr.scr_a_vencer_outros_emprestimos,
            scr.scr_a_vencer_antecipacao_de_fatura_de_cartao_de_credito,
            scr.scr_a_vencer_aquisicao_veiculos_automotores,
            scr.scr_a_vencer_aquisicao_outros_bens,
            scr.scr_a_vencer_cartao_de_credito_compra_ou_fatura_parcelada,
            scr.scr_a_vencer_veiculos_automotores_acima_2_ton,
            scr.scr_a_vencer_outros_financiamentos,
            scr.scr_a_vencer_financiamento_habitacional_sfh,
            scr.scr_a_vencer_financiamento_habitacional_exceto_sfh,
            scr.scr_a_vencer_financiamento_empreendimento_nao_habitacional,
            scr.scr_a_vencer_titulos_e_creditos_a_receber,
            scr.scr_a_vencer_cartao_de_credito_compra_a_vista_e_parcelado_lojista,
            scr.scr_a_vencer_recebiveis_de_arranjo_de_pagamento,
            scr.scr_a_vencer_limite_contratado_nao_utilizado,
            scr.scr_a_vencer_retencao_de_risco_cotas_de_fundos,
            scr.scr_a_vencer_retencao_de_risco_instrumentos_com_lastros_em_operacoes_de_credito
            ,
            scr.scr_vencido_adiantamentos_a_depositantes,
            scr.scr_vencido_cheque_especial_e_conta_garantida,
            scr.scr_vencido_credito_pessoal_consignado,
            scr.scr_vencido_credito_pessoal_sem_consignado,
            scr.scr_vencido_credito_rotativo_cartao_de_credito,
            scr.scr_vencido_cartao_de_credito_compra_fatura_ou_saque_financiado,
            scr.scr_vencido_home_equity,
            scr.scr_vencido_microcredito,
            scr.scr_vencido_cheque_especial,
            scr.scr_vencido_outros_emprestimos,
            scr.scr_vencido_antecipacao_de_fatura_de_cartao_de_credito,
            scr.scr_vencido_aquisicao_veiculos_automotores,
            scr.scr_vencido_aquisicao_outros_bens,
            scr.scr_vencido_cartao_de_credito_compra_ou_fatura_parcelada,
            scr.scr_vencido_veiculos_automotores_acima_2_ton,
            scr.scr_vencido_outros_financiamentos,
            scr.scr_vencido_financiamento_habitacional_sfh,
            scr.scr_vencido_financiamento_habitacional_exceto_sfh,
            scr.scr_vencido_financiamento_empreendimento_nao_habitacional,
            scr.scr_vencido_titulos_e_creditos_a_receber,
            scr.scr_vencido_cartao_de_credito_compra_a_vista_e_parcelado_lojista,
            scr.scr_vencido_recebiveis_de_arranjo_de_pagamento,
            scr.scr_vencido_limite_contratado_nao_utilizado,
            scr.scr_vencido_retencao_de_risco_cotas_de_fundos,
            scr.scr_vencido_retencao_de_risco_instrumentos_com_lastros_em_operacoes_de_credito
            ,
            abs(
                datediff(
                    'days',
                    pc.pre_analysis_created_at,
                    dateadd('hours', -3, scr.scr_report_consulted_at)
                )
            ) as days_from_consultation,
            row_number() over (
                partition by pc.pre_analysis_id order by days_from_consultation
            ) as credit_check_order
        from complete_pre_analysis as pc
        left join
            {{ ref('source_credit_checks_api_scr_report') }} as scr
            on pc.hash_cpf = scr.hash_cpf

    ),
    scr_report_c_c as (

        select
            pre_analysis_id,
            pre_analysis_created_at,
            hash_cpf,
            pre_analysis_type,
            has_error,
            case
                when days_from_consultation between 0 and 15
                then scr_report_consulted_at
                else null
            end as scr_report_consulted_at,
            case
                when days_from_consultation between 0 and 15
                then scr_mensagem_operador
                else null
            end as scr_mensagem_operador,
            case
                when days_from_consultation between 0 and 15
                then scr_data_inicio_relacionamento
                else null
            end as scr_data_inicio_relacionamento,
            case
                when days_from_consultation between 0 and 15 then scr_prejuizo else null
            end as scr_prejuizo,
            case
                when days_from_consultation between 0 and 15 then scr_repasses else null
            end as scr_repasses,
            case
                when days_from_consultation between 0 and 15
                then scr_risco_total
                else null
            end as scr_risco_total,
            case
                when days_from_consultation between 0 and 15
                then scr_coobrigacoes
                else null
            end as scr_coobrigacoes,
            case
                when days_from_consultation between 0 and 15
                then scr_qtd_de_operacoes
                else null
            end as scr_qtd_de_operacoes,
            case
                when days_from_consultation between 0 and 15
                then scr_carteira_vencer
                else null
            end as scr_carteira_vencer,
            case
                when days_from_consultation between 0 and 15
                then scr_carteira_vencido
                else null
            end as scr_carteira_vencido,
            case
                when days_from_consultation between 0 and 15
                then scr_creditos_a_liberar
                else null
            end as scr_creditos_a_liberar,
            case
                when days_from_consultation between 0 and 15
                then scr_limites_de_credito
                else null
            end as scr_limites_de_credito,
            case
                when days_from_consultation between 0 and 15
                then scr_qtd_de_instituicoes
                else null
            end as scr_qtd_de_instituicoes,
            case
                when days_from_consultation between 0 and 15
                then scr_vlr_operacoes_sob_judice
                else null
            end as scr_vlr_operacoes_sob_judice,
            case
                when days_from_consultation between 0 and 15
                then scr_qtd_operacoes_sob_judice
                else null
            end as scr_qtd_operacoes_sob_judice,
            case
                when days_from_consultation between 0 and 15
                then scr_carteira_a_vencer_31_a_60_dias
                else null
            end as scr_carteira_a_vencer_31_a_60_dias,
            case
                when days_from_consultation between 0 and 15
                then scr_carteira_a_vencer_61_a_90_dias
                else null
            end as scr_carteira_a_vencer_61_a_90_dias,
            case
                when days_from_consultation between 0 and 15
                then scr_carteira_a_vencer_91_a_180_dias
                else null
            end as scr_carteira_a_vencer_91_a_180_dias,
            case
                when days_from_consultation between 0 and 15
                then scr_carteira_vencido_15_a_30_dias
                else null
            end as scr_carteira_vencido_15_a_30_dias,
            case
                when days_from_consultation between 0 and 15
                then scr_carteira_vencido_31_a_60_dias
                else null
            end as scr_carteira_vencido_31_a_60_dias,
            case
                when days_from_consultation between 0 and 15
                then scr_carteira_vencido_61_a_90_dias
                else null
            end as scr_carteira_vencido_61_a_90_dias,
            case
                when days_from_consultation between 0 and 15
                then scr_vlr_operacoes_discordancia
                else null
            end as scr_vlr_operacoes_discordancia,
            case
                when days_from_consultation between 0 and 15
                then scr_carteira_a_vencer_181_a_360_dias
                else null
            end as scr_carteira_a_vencer_181_a_360_dias,
            case
                when days_from_consultation between 0 and 15
                then scr_carteira_vencido_91_a_180_dias
                else null
            end as scr_carteira_vencido_91_a_180_dias,
            case
                when days_from_consultation between 0 and 15
                then scr_perc_documentos_processados
                else null
            end as scr_perc_documentos_processados,
            case
                when days_from_consultation between 0 and 15
                then scr_qtd_de_operacoes_discordancia
                else null
            end as scr_qtd_de_operacoes_discordancia,
            case
                when days_from_consultation between 0 and 15
                then scr_carteira_a_vencer_acima_360_dias
                else null
            end as scr_carteira_a_vencer_acima_360_dias,
            case
                when days_from_consultation between 0 and 15
                then scr_carteira_vencido_181_a_360_dias
                else null
            end as scr_carteira_vencido_181_a_360_dias,
            case
                when days_from_consultation between 0 and 15
                then scr_limites_de_credito_ate_360_dias
                else null
            end as scr_limites_de_credito_ate_360_dias,
            case
                when days_from_consultation between 0 and 15
                then scr_carteira_vencido_acima_360_dias
                else null
            end as scr_carteira_vencido_acima_360_dias,
            case
                when days_from_consultation between 0 and 15
                then scr_limites_de_credito_acima_360_dias
                else null
            end as scr_limites_de_credito_acima_360_dias,
            case
                when days_from_consultation between 0 and 15
                then scr_carteira_a_vencer_prazo_indeterminado
                else null
            end as scr_carteira_a_vencer_prazo_indeterminado,
            case
                when days_from_consultation between 0 and 15
                then scr_carteira_a_vencer_ate_30_dias_vencidos_ate_14_dias
                else null
            end as scr_carteira_a_vencer_ate_30_dias_vencidos_ate_14_dias,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_adiantamentos_a_depositantes
                else null
            end as scr_a_vencer_adiantamentos_a_depositantes,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_cheque_especial_e_conta_garantida
                else null
            end as scr_a_vencer_cheque_especial_e_conta_garantida,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_credito_pessoal_consignado
                else null
            end as scr_a_vencer_credito_pessoal_consignado,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_credito_pessoal_sem_consignado
                else null
            end as scr_a_vencer_credito_pessoal_sem_consignado,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_credito_rotativo_cartao_de_credito
                else null
            end as scr_a_vencer_credito_rotativo_cartao_de_credito,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_cartao_de_credito_compra_fatura_ou_saque_financiado
                else null
            end as scr_a_vencer_cartao_de_credito_compra_fatura_ou_saque_financiado,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_home_equity
                else null
            end as scr_a_vencer_home_equity,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_microcredito
                else null
            end as scr_a_vencer_microcredito,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_cheque_especial
                else null
            end as scr_a_vencer_cheque_especial,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_outros_emprestimos
                else null
            end as scr_a_vencer_outros_emprestimos,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_antecipacao_de_fatura_de_cartao_de_credito
                else null
            end as scr_a_vencer_antecipacao_de_fatura_de_cartao_de_credito,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_aquisicao_veiculos_automotores
                else null
            end as scr_a_vencer_aquisicao_veiculos_automotores,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_aquisicao_outros_bens
                else null
            end as scr_a_vencer_aquisicao_outros_bens,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_cartao_de_credito_compra_ou_fatura_parcelada
                else null
            end as scr_a_vencer_cartao_de_credito_compra_ou_fatura_parcelada,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_veiculos_automotores_acima_2_ton
                else null
            end as scr_a_vencer_veiculos_automotores_acima_2_ton,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_outros_financiamentos
                else null
            end as scr_a_vencer_outros_financiamentos,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_financiamento_habitacional_sfh
                else null
            end as scr_a_vencer_financiamento_habitacional_sfh,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_financiamento_habitacional_exceto_sfh
                else null
            end as scr_a_vencer_financiamento_habitacional_exceto_sfh,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_financiamento_empreendimento_nao_habitacional
                else null
            end as scr_a_vencer_financiamento_empreendimento_nao_habitacional,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_titulos_e_creditos_a_receber
                else null
            end as scr_a_vencer_titulos_e_creditos_a_receber,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_cartao_de_credito_compra_a_vista_e_parcelado_lojista
                else null
            end as scr_a_vencer_cartao_de_credito_compra_a_vista_e_parcelado_lojista,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_recebiveis_de_arranjo_de_pagamento
                else null
            end as scr_a_vencer_recebiveis_de_arranjo_de_pagamento,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_limite_contratado_nao_utilizado
                else null
            end as scr_a_vencer_limite_contratado_nao_utilizado,
            case
                when days_from_consultation between 0 and 15
                then scr_a_vencer_retencao_de_risco_cotas_de_fundos
                else null
            end as scr_a_vencer_retencao_de_risco_cotas_de_fundos,
            case
                when days_from_consultation between 0 and 15
                then
                    scr_a_vencer_retencao_de_risco_instrumentos_com_lastros_em_operacoes_de_credito
                else null
            end
            as scr_a_vencer_retencao_de_risco_instrumentos_com_lastros_em_operacoes_de_credito
            ,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_adiantamentos_a_depositantes
                else null
            end as scr_vencido_adiantamentos_a_depositantes,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_cheque_especial_e_conta_garantida
                else null
            end as scr_vencido_cheque_especial_e_conta_garantida,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_credito_pessoal_consignado
                else null
            end as scr_vencido_credito_pessoal_consignado,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_credito_pessoal_sem_consignado
                else null
            end as scr_vencido_credito_pessoal_sem_consignado,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_credito_rotativo_cartao_de_credito
                else null
            end as scr_vencido_credito_rotativo_cartao_de_credito,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_cartao_de_credito_compra_fatura_ou_saque_financiado
                else null
            end as scr_vencido_cartao_de_credito_compra_fatura_ou_saque_financiado,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_home_equity
                else null
            end as scr_vencido_home_equity,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_microcredito
                else null
            end as scr_vencido_microcredito,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_cheque_especial
                else null
            end as scr_vencido_cheque_especial,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_outros_emprestimos
                else null
            end as scr_vencido_outros_emprestimos,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_antecipacao_de_fatura_de_cartao_de_credito
                else null
            end as scr_vencido_antecipacao_de_fatura_de_cartao_de_credito,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_aquisicao_veiculos_automotores
                else null
            end as scr_vencido_aquisicao_veiculos_automotores,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_aquisicao_outros_bens
                else null
            end as scr_vencido_aquisicao_outros_bens,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_cartao_de_credito_compra_ou_fatura_parcelada
                else null
            end as scr_vencido_cartao_de_credito_compra_ou_fatura_parcelada,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_veiculos_automotores_acima_2_ton
                else null
            end as scr_vencido_veiculos_automotores_acima_2_ton,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_outros_financiamentos
                else null
            end as scr_vencido_outros_financiamentos,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_financiamento_habitacional_sfh
                else null
            end as scr_vencido_financiamento_habitacional_sfh,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_financiamento_habitacional_exceto_sfh
                else null
            end as scr_vencido_financiamento_habitacional_exceto_sfh,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_financiamento_empreendimento_nao_habitacional
                else null
            end as scr_vencido_financiamento_empreendimento_nao_habitacional,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_titulos_e_creditos_a_receber
                else null
            end as scr_vencido_titulos_e_creditos_a_receber,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_cartao_de_credito_compra_a_vista_e_parcelado_lojista
                else null
            end as scr_vencido_cartao_de_credito_compra_a_vista_e_parcelado_lojista,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_recebiveis_de_arranjo_de_pagamento
                else null
            end as scr_vencido_recebiveis_de_arranjo_de_pagamento,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_limite_contratado_nao_utilizado
                else null
            end as scr_vencido_limite_contratado_nao_utilizado,
            case
                when days_from_consultation between 0 and 15
                then scr_vencido_retencao_de_risco_cotas_de_fundos
                else null
            end as scr_vencido_retencao_de_risco_cotas_de_fundos,
            case
                when days_from_consultation between 0 and 15
                then
                    scr_vencido_retencao_de_risco_instrumentos_com_lastros_em_operacoes_de_credito
                else null
            end
            as scr_vencido_retencao_de_risco_instrumentos_com_lastros_em_operacoes_de_credito
            ,
            days_from_consultation,
            credit_check_order
        from scr_report_prep
        where credit_check_order = 1

    ),
    pre_analysis_credit_check as (

        select

            /* Pre Anlysis Information */
            pc.pre_analysis_id,
            pc.pre_analysis_type,
            pc.pre_analysis_created_at,
            pc.hash_cpf,
            pc.cpf_seventh_and_eighth_number,
            coalesce(cei.crivo_id, socc.crivo_id) as crivo_id,
            cei.crivo_patient_id,
            case
                when
                    timestampdiff(
                        'day', s.serasa_consulted_at, pc.pre_analysis_created_at
                    )
                    between 1 and 15
                then 'cache'
                else null
            end is_cache_serasa,
            case
                when
                    timestampdiff(
                        'day',
                        bvs.bvs_score_pf_net_consulted_at,
                        pc.pre_analysis_created_at
                    )
                    between 1 and 15
                then 'cache'
                else null
            end is_cache_bvs_score_pf,
            case
                when
                    timestampdiff(
                        'day', scr.scr_report_consulted_at, pc.pre_analysis_created_at
                    )
                    between 1 and 15
                then 'cache'
                else null
            end is_cache_scr,
            case
                when
                    timestampdiff(
                        'day',
                        scpc.bvs_scpc_net_consulted_at,
                        pc.pre_analysis_created_at
                    )
                    between 1 and 15
                then 'cache'
                else null
            end is_cache_bvs_scpc,
            sccc.clinic_credit_score as crivo_clinic_credit_score,

            /* Serasa */
            coalesce(s.serasa_consulted_at, socc.serasa_consulted_at) as
            serasa_consulted_at,
            coalesce(s.serasa_ir_status, socc.serasa_status) as serasa_ir_status,
            coalesce(s.serasa_ccf, socc.serasa_ccf) as serasa_ccf,
            s.has_error as serasa_has_error,
            coalesce(s.serasa_positive_score, socc.serasa_positive_score) as
            serasa_positive_score,
            coalesce(s.serasa_refin, socc.serasa_refin) as serasa_refin,
            coalesce(s.serasa_pefin, socc.serasa_pefin) as serasa_pefin,
            coalesce(s.serasa_protest, socc.serasa_protest) as serasa_protest,
            coalesce(s.serasa_presumed_income, socc.serasa_presumed_income) as
            serasa_presumed_income,

            /* BVS */
            bvs.bvs_score_pf_net_consulted_at as bvs_score_pf_net_consulted_at,
            coalesce(bvs.bvs_positive_score, socc.bvs_positive_score) as
            bvs_positive_score,
            bvs.has_error as bvs_has_error,
            scpc.has_error as scpc_has_error,
            coalesce(scpc.bvs_scpc_net_consulted_at, socc.bvs_consulted_at) as
            bvs_scpc_net_consulted_at,
            coalesce(scpc.bvs_ccf_count, socc.ccf_bvs) as bvs_ccf_count,
            coalesce(scpc.bvs_total_debt, socc.bvs_debit) as bvs_total_debt,
            coalesce(scpc.bvs_total_protest, socc.bvs_protest) as bvs_total_protest,
            coalesce(scpc.bvs_status_ir, socc.bvs_status) as bvs_status_ir,

            -- /* SCR */
            sccc.score_scr,
            scr.scr_report_consulted_at,
            scr.has_error as scr_has_error,
            scr.scr_mensagem_operador,
            scr.scr_data_inicio_relacionamento,
            scr.scr_prejuizo,
            scr.scr_repasses,
            scr.scr_risco_total,
            scr.scr_coobrigacoes,
            scr.scr_qtd_de_operacoes,
            scr.scr_carteira_vencer,
            scr.scr_carteira_vencido,
            scr.scr_creditos_a_liberar,
            scr.scr_limites_de_credito,
            scr.scr_qtd_de_instituicoes,
            scr.scr_vlr_operacoes_sob_judice,
            scr.scr_qtd_operacoes_sob_judice,
            scr.scr_carteira_a_vencer_31_a_60_dias,
            scr.scr_carteira_a_vencer_61_a_90_dias,
            scr.scr_carteira_a_vencer_91_a_180_dias,
            scr.scr_carteira_vencido_15_a_30_dias,
            scr.scr_carteira_vencido_31_a_60_dias,
            scr.scr_carteira_vencido_61_a_90_dias,
            scr.scr_vlr_operacoes_discordancia,
            scr.scr_carteira_a_vencer_181_a_360_dias,
            scr.scr_carteira_vencido_91_a_180_dias,
            scr.scr_perc_documentos_processados,
            scr.scr_qtd_de_operacoes_discordancia,
            scr.scr_carteira_a_vencer_acima_360_dias,
            scr.scr_carteira_vencido_181_a_360_dias,
            scr.scr_limites_de_credito_ate_360_dias,
            scr.scr_carteira_vencido_acima_360_dias,
            scr.scr_limites_de_credito_acima_360_dias,
            scr.scr_carteira_a_vencer_prazo_indeterminado,
            scr.scr_carteira_a_vencer_ate_30_dias_vencidos_ate_14_dias,
            scr.scr_a_vencer_adiantamentos_a_depositantes,
            scr.scr_a_vencer_cheque_especial_e_conta_garantida,
            scr.scr_a_vencer_credito_pessoal_consignado,
            scr.scr_a_vencer_credito_pessoal_sem_consignado,
            scr.scr_a_vencer_credito_rotativo_cartao_de_credito,
            scr.scr_a_vencer_cartao_de_credito_compra_fatura_ou_saque_financiado,
            scr.scr_a_vencer_home_equity,
            scr.scr_a_vencer_microcredito,
            scr.scr_a_vencer_cheque_especial,
            scr.scr_a_vencer_outros_emprestimos,
            scr.scr_a_vencer_antecipacao_de_fatura_de_cartao_de_credito,
            scr.scr_a_vencer_aquisicao_veiculos_automotores,
            scr.scr_a_vencer_aquisicao_outros_bens,
            scr.scr_a_vencer_cartao_de_credito_compra_ou_fatura_parcelada,
            scr.scr_a_vencer_veiculos_automotores_acima_2_ton,
            scr.scr_a_vencer_outros_financiamentos,
            scr.scr_a_vencer_financiamento_habitacional_sfh,
            scr.scr_a_vencer_financiamento_habitacional_exceto_sfh,
            scr.scr_a_vencer_financiamento_empreendimento_nao_habitacional,
            scr.scr_a_vencer_titulos_e_creditos_a_receber,
            scr.scr_a_vencer_cartao_de_credito_compra_a_vista_e_parcelado_lojista,
            scr.scr_a_vencer_recebiveis_de_arranjo_de_pagamento,
            scr.scr_a_vencer_limite_contratado_nao_utilizado,
            scr.scr_a_vencer_retencao_de_risco_cotas_de_fundos,
            scr.scr_a_vencer_retencao_de_risco_instrumentos_com_lastros_em_operacoes_de_credito,
            scr.scr_vencido_adiantamentos_a_depositantes,
            scr.scr_vencido_cheque_especial_e_conta_garantida,
            scr.scr_vencido_credito_pessoal_consignado,
            scr.scr_vencido_credito_pessoal_sem_consignado,
            scr.scr_vencido_credito_rotativo_cartao_de_credito,
            scr.scr_vencido_cartao_de_credito_compra_fatura_ou_saque_financiado,
            scr.scr_vencido_home_equity,
            scr.scr_vencido_microcredito,
            scr.scr_vencido_cheque_especial,
            scr.scr_vencido_outros_emprestimos,
            scr.scr_vencido_antecipacao_de_fatura_de_cartao_de_credito,
            scr.scr_vencido_aquisicao_veiculos_automotores,
            scr.scr_vencido_aquisicao_outros_bens,
            scr.scr_vencido_cartao_de_credito_compra_ou_fatura_parcelada,
            scr.scr_vencido_veiculos_automotores_acima_2_ton,
            scr.scr_vencido_outros_financiamentos,
            scr.scr_vencido_financiamento_habitacional_sfh,
            scr.scr_vencido_financiamento_habitacional_exceto_sfh,
            scr.scr_vencido_financiamento_empreendimento_nao_habitacional,
            scr.scr_vencido_titulos_e_creditos_a_receber,
            scr.scr_vencido_cartao_de_credito_compra_a_vista_e_parcelado_lojista,
            scr.scr_vencido_recebiveis_de_arranjo_de_pagamento,
            scr.scr_vencido_limite_contratado_nao_utilizado,
            scr.scr_vencido_retencao_de_risco_cotas_de_fundos,
            scr.scr_vencido_retencao_de_risco_instrumentos_com_lastros_em_operacoes_de_credito

        from complete_pre_analysis as pc
        left join
            {{ ref('source_credit_engine_information' )}} as cei
            on cei.engineable_id = pc.pre_analysis_id
            and cei.engineable_type = 'pre_analysis'
            and cei.row_number_desc = 1
        left join
            {{ ref('source_crivo_credit_checks' )}} as sccc
            on cei.crivo_id = sccc.crivo_id
        left join
            (
                select *
                from {{ ref('source_crivo_checks') }}
                qualify
                    row_number() over (
                        partition by engineable_id
                        order by
                            crivo_check_created_at desc,
                            crivo_check_updated_at desc,
                            crivo_check_id desc
                    )
                    = 1
            ) socc
            on pc.pre_analysis_id = socc.engineable_id
        left join serasa_c_c as s
            on  s.pre_analysis_id   = pc.pre_analysis_id
            and s.pre_analysis_type = pc.pre_analysis_type
        left join bvs_score_pf_c_c as bvs
            on  bvs.pre_analysis_id   = pc.pre_analysis_id
            and bvs.pre_analysis_type = pc.pre_analysis_type
        left join bvs_scpc_c_c as scpc
            on  scpc.pre_analysis_id   = pc.pre_analysis_id
            and scpc.pre_analysis_type = pc.pre_analysis_type
        left join scr_report_c_c as scr
            on  scr.pre_analysis_id   = pc.pre_analysis_id
            and scr.pre_analysis_type = pc.pre_analysis_type

    )

select *
from pre_analysis_credit_check