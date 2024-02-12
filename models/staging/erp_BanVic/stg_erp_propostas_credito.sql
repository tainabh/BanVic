with
    fonte_propostas_credito as (
        select 
            cast (cod_proposta as int) as id_proposta
            , cast (cod_cliente as int) as id_cliente
            , cast (cod_colaborador as int) as id_colaborador
            , cast ((date (data_entrada_proposta)) as date) as data_entrada_da_proposta
            , cast (taxa_juros_mensal as numeric) as taxa_de_juros_mensal
            , cast (valor_proposta as numeric) as valor_da_proposta
            , cast (valor_financiamento as numeric) as valor_do_financiamento
            , cast (valor_entrada as numeric) as valor_de_entrada
            , cast (valor_prestacao as numeric) as valor_da_prestacao
            , cast (quantidade_parcelas as int) as quantidade_de_parcelas
            , cast (carencia as numeric) as periodo_carencia
            , cast (status_proposta as string) as status_da_proposta
        from {{ source('erp', 'Propostas_credito') }}
    )

select *
from fonte_propostas_credito