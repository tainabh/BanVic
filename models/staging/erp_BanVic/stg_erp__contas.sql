with
    fonte_contas as (
        select 
            cast (cod_cliente as int) as id_cliente
            , cast (cod_agencia as int) as id_agencia
            , cast (cod_colaborador as int) as id_colaborador
            , cast (num_conta as int) as numero_da_conta
            , cast (tipo_conta as string) as tipo_de_conta
            , cast ((date (data_abertura)) as date) as data_abertura_conta
            , cast (saldo_total as numeric) as saldo_total_conta
            , cast (saldo_disponivel as numeric) as saldo_disponivel_conta
            , cast ((date (data_ultimo_lancamento)) as date) as data_ultimo_lancamento_conta
        from {{ source('erp', 'Contas') }}
    )

select *
from fonte_contas 