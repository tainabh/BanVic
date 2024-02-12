with
    stg_clientes as (
        select 
            id_cliente
            , nome_cliente
            , tipo_de_cliente
            , data_inclusao_cliente
            , data_nascimento_cliente
            , endereco_cliente
            , cep_cliente
        from {{ ref('stg_erp__clientes') }}
    )

select *
from stg_clientes