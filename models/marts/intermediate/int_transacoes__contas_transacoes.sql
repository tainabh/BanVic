with
    stg_contas as (
        select 
            id_cliente
            , id_agencia
            , id_colaborador
            , numero_da_conta
            , tipo_de_conta
            , data_abertura_conta
            , saldo_total_conta
            , saldo_disponivel_conta
            , data_ultimo_lancamento_conta
        from {{ ref('stg_erp__contas') }}
    )

    , stg_transacoes as (
        select 
            id_transacao
            , tipo_transacao
            , numero_da_conta
            , data_da_transacao
            , valor_da_transacao
        from {{ ref('stg_erp__transacoes') }}
    )

    , joined_tabelas as (
        select
            stg_transacoes.id_transacao
            , stg_contas.id_cliente
            , stg_contas.id_agencia
            , stg_contas.id_colaborador
            , stg_transacoes.tipo_transacao
            --, stg_transacoes.numero_da_conta
            , stg_transacoes.data_da_transacao
            , stg_transacoes.valor_da_transacao            
            , stg_contas.numero_da_conta
            , stg_contas.tipo_de_conta
            , stg_contas.data_abertura_conta
            , stg_contas.saldo_total_conta
            , stg_contas.saldo_disponivel_conta
            , stg_contas.data_ultimo_lancamento_conta
        from stg_transacoes
        left join stg_contas on
            stg_transacoes.numero_da_conta = stg_contas.numero_da_conta
        
    )

    , criar_chave as (
        select
            id_transacao|| '-' ||data_da_transacao|| '-' ||numero_da_conta as sk_contas_transacoes
            , id_transacao
            , id_cliente
            , id_agencia
            , id_colaborador
            , tipo_transacao
            , data_da_transacao
            , valor_da_transacao
            , numero_da_conta
            , tipo_de_conta
            , data_abertura_conta
            , saldo_total_conta
            , saldo_disponivel_conta
            , data_ultimo_lancamento_conta
        from joined_tabelas
    )

select *
from criar_chave