with
    fonte_clientes as (
        select 
            cast (cod_cliente as int) as id_cliente
            , cast (primeiro_nome as string) || ' ' || cast (ultimo_nome as string) as nome_cliente
            --, cast (email as string) - nao aplicavel as regras de negocio
            , cast (tipo_cliente as string) as tipo_de_cliente
            , cast ((date (data_inclusao)) as date) as data_inclusao_cliente
            --, cast (cpfcnpj as string) - nao aplicavel as regras de negocio
            , cast (data_nascimento as date) as data_nascimento_cliente
            , cast (endereco as string) as endereco_cliente
            , cast (cep as string) as cep_cliente
        
        from {{ source('erp', 'Clientes') }}
    )

select *
from fonte_clientes 