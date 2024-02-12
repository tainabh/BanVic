with
    clientes as (
        select 
            *
        from {{ ref('dim_clientes') }}
    )

    , colaboradores as (
        select 
            *
        from {{ ref('dim_colaborador') }}
    )

    , propostas as (
        select 
            *
        from {{ ref('dim_propostas_credito') }}
    )

    , contas_transacoes as (
        select 
            *
        from {{ ref('int_transacoes__contas_transacoes') }}
    )

    , joined_tabelas as (
        select

            -- CHAVES --
            contas_transacoes.sk_contas_transacoes
            , contas_transacoes.id_transacao
            , propostas.id_proposta
            , contas_transacoes.id_cliente
            , row_number() over (partition by contas_transacoes.id_transacao order by propostas.id_proposta) AS rn  
            , contas_transacoes.id_colaborador
            , contas_transacoes.id_agencia

            -- DATAS --
            , contas_transacoes.data_abertura_conta
            , contas_transacoes.data_da_transacao
            , contas_transacoes.data_ultimo_lancamento_conta
            , propostas.data_entrada_da_proposta
            , colaboradores.data_nascimento_colaborador
            , clientes.data_inclusao_cliente
            , clientes.data_nascimento_cliente
                       
            -- METRICAS --
            , contas_transacoes.valor_da_transacao 
            , contas_transacoes.saldo_total_conta
            , contas_transacoes.saldo_disponivel_conta            
            , propostas.valor_da_proposta
            , propostas.valor_do_financiamento
            , propostas.valor_de_entrada
            , propostas.valor_da_prestacao
            , propostas.quantidade_de_parcelas
            , propostas.periodo_carencia         
            , propostas.taxa_de_juros_mensal 
                  
            -- DESCRICOES --
            , clientes.nome_cliente
            , clientes.tipo_de_cliente
            , clientes.endereco_cliente
            , clientes.cep_cliente
            , contas_transacoes.numero_da_conta
            , contas_transacoes.tipo_de_conta
            , colaboradores.nome_colaborador
            , colaboradores.endereco_colaborador
            , colaboradores.cep_colaborador
            , colaboradores.nome_agencia
            , colaboradores.tipo_agencia
            , colaboradores.endereco_agencia
            , colaboradores.cidade_agencia
            , colaboradores.uf_agencia
            , contas_transacoes.tipo_transacao
            , propostas.status_da_proposta

        from contas_transacoes
        left join clientes on
            contas_transacoes.id_cliente = clientes.id_cliente
        left join colaboradores on
            contas_transacoes.id_colaborador = colaboradores.id_colaborador
        left join propostas on 
            contas_transacoes.id_colaborador = propostas.id_colaborador
        
        
    )


    , transformacoes as (
        select
            *
            , saldo_total_conta / count (id_transacao) over (partition by id_cliente) as saldo_total_conta_ponderado
            , saldo_disponivel_conta / count (id_transacao) over (partition by id_cliente) as saldo_disponivel_conta_ponderado
            
        from joined_tabelas
        where rn = 1
    )

    , select_final as (
        select
            -- CHAVES --
            sk_contas_transacoes
            , id_transacao
            , id_proposta
            , id_cliente        
            , id_colaborador
            , id_agencia

            -- DATAS --

            , data_abertura_conta
            , data_da_transacao
            , data_ultimo_lancamento_conta
            , data_entrada_da_proposta
            , data_nascimento_colaborador
            , data_inclusao_cliente
            , data_nascimento_cliente

            -- METRICAS --
            , valor_da_transacao
            , saldo_total_conta_ponderado
            , saldo_disponivel_conta_ponderado
            , valor_da_proposta
            , valor_do_financiamento
            , valor_de_entrada
            , valor_da_prestacao
            , quantidade_de_parcelas
            , periodo_carencia
            , taxa_de_juros_mensal

            -- DESCRICOES --
            , nome_cliente
            , tipo_de_cliente
            , endereco_cliente
            , cep_cliente
            , numero_da_conta
            , tipo_de_conta
            , nome_colaborador
            , endereco_colaborador
            , cep_colaborador
            , nome_agencia
            , tipo_agencia
            , endereco_agencia
            , cidade_agencia
            , uf_agencia
            , tipo_transacao
            , status_da_proposta
            , rn
        from transformacoes
    )

select *
from select_final
