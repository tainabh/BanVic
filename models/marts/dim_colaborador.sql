with
    stg_colaborador as (
        select 
            id_colaborador
            , nome_colaborador
            , data_nascimento_colaborador
            , endereco_colaborador
            , cep_colaborador
        from {{ ref('stg_erp__colaboradores') }}
    )

    , stg_agencia as (
        select 
            id_agencia
            , nome_agencia
            , tipo_agencia
            , endereco_agencia
            , cidade_agencia
            , uf_agencia
            , data_abaertura_agencia
        from {{ ref('stg_erp__agencias') }}
    )

    , stg_colaborador_agencia as (
        select 
            id_colaborador
            , id_agencia
        from {{ ref('stg_erp__colaborador_agencia') }}
    )

    , joined_tabelas as (
        select
            stg_colaborador.id_colaborador
            , stg_agencia.id_agencia
            , stg_colaborador.nome_colaborador
            , stg_colaborador.data_nascimento_colaborador
            , stg_colaborador.endereco_colaborador
            , stg_colaborador.cep_colaborador            
            , stg_agencia.nome_agencia
            , stg_agencia.tipo_agencia
            , stg_agencia.endereco_agencia
            , stg_agencia.cidade_agencia
            , stg_agencia.uf_agencia
        from stg_colaborador
        left join stg_colaborador_agencia on
            stg_colaborador.id_colaborador = stg_colaborador_agencia.id_colaborador
        left join stg_agencia on
            stg_colaborador_agencia.id_agencia = stg_agencia.id_agencia
    )

select *
from joined_tabelas