with
    stg_propostas_credito as (
        select 
            id_proposta
            , id_cliente
            , id_colaborador
            , data_entrada_da_proposta
            , taxa_de_juros_mensal
            , valor_da_proposta
            , valor_do_financiamento
            , valor_de_entrada
            , valor_da_prestacao
            , quantidade_de_parcelas
            , periodo_carencia
            , status_da_proposta
        from {{ ref('stg_erp_propostas_credito') }}
    )

select *
from stg_propostas_credito