with
    fonte_colaboradores as (
        select 
            cast (cod_colaborador as int) as id_colaborador
            , cast (primeiro_nome as string) || ' ' || cast (ultimo_nome as string) as nome_colaborador
            --, cast (email as string) - nao aplicavel as regras de negocio
            -- , cast (cpf as string) - nao aplicavel as regras de negocio
            , cast (data_nascimento as date) as data_nascimento_colaborador
            , cast (endereco as string) as endereco_colaborador
            , cast (cep as string) as cep_colaborador

        from {{ source('erp', 'Colaboradores') }}
    )

select *
from fonte_colaboradores 