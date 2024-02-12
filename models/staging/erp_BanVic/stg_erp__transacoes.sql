with
    fonte_transacoes as (
        select 
            cast (cod_transacao as int) as id_transacao
            , cast (nome_transacao as string) as tipo_transacao
            , cast (num_conta as int) as numero_da_conta
            , cast ((date (data_transacao)) as date) as data_da_transacao             
            , cast (valor_transacao as numeric) as valor_da_transacao
        from {{ source('erp', 'Transacoes') }}
    )

select *
from fonte_transacoes