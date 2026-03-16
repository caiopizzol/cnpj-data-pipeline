EMPRESA = """
SELECT
    jsonb_build_object(
        'taxId', est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv,
        'alias', est.nome_fantasia, 
        'founded', est.data_inicio_atividade, 
        'head', (est.cnpj_ordem = '0001'),
        'company', jsonb_build_object(
            'id', est.cnpj_basico,
            'name', emp.razao_social,
            'equity', emp.capital_social,
            'members', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'since', soc.data_entrada_sociedade,
                    'name', soc.nome_socio
                ))
                FROM public.socios soc
                WHERE soc.cnpj_basico = est.cnpj_basico
            ), '[]'::jsonb),
            'nature', jsonb_build_object(
                'id', emp.natureza_juridica,
                'text', nat.descricao
            ),
            'size', jsonb_build_object(
                'id', CAST(NULLIF(emp.porte, '') AS INTEGER),
                'acronym', CASE
                    WHEN sim.opcao_pelo_mei = 'S' THEN 'MEI'
                    WHEN emp.porte = '01' THEN 'ME'
                    WHEN emp.porte = '03' THEN 'EPP'
                    ELSE 'OUT'
                END,
                'text', CASE 
                    WHEN sim.opcao_pelo_mei = 'S' THEN 'Microempreendedor Individual'
                    WHEN emp.porte = '01' THEN 'Microempresa'
                    WHEN emp.porte = '03' THEN 'Empresa de Pequeno Porte'
                    ELSE 'Empresas de Médio e Grande Porte'
                END
            ),
            'simples', jsonb_build_object(
                'optant', (sim.opcao_pelo_simples = 'S'),
                'since', sim.data_opcao_pelo_simples,
                'to', sim.data_exclusao_do_simples
            ),
            'simei', jsonb_build_object(
                'optant', (sim.opcao_pelo_mei = 'S'),
                'since', sim.data_opcao_pelo_mei,
                'to', sim.data_exclusao_do_mei
            )
        ),
        'statusDate', est.data_situacao_cadastral,
        'status', jsonb_build_object(
            'id', CAST(NULLIF(est.situacao_cadastral, '') AS INTEGER),
            'text', CASE
                WHEN est.situacao_cadastral = '01' THEN 'Nula'
                WHEN est.situacao_cadastral = '02' THEN 'Ativa'
                WHEN est.situacao_cadastral = '03' THEN 'Suspensa'
                WHEN est.situacao_cadastral = '04' THEN 'Inapta'
                WHEN est.situacao_cadastral = '08' THEN 'Baixada'
            END
        ),
        'address', jsonb_build_object(
            'municipality', est.municipio,
            'street', TRIM(COALESCE(est.tipo_logradouro, '') || ' ' || COALESCE(est.logradouro, '')),
            'number', est.numero,
            'district', est.bairro,
            'city', mun.descricao,
            'state', est.uf,
            'details', est.complemento,
            'zip', est.cep
        ),
        'mainActivity', jsonb_build_object(
            'id', est.cnae_fiscal_principal,
            'text', cnaep.descricao
        ),
        'sideActivities', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'id', cnae.codigo,
                'text', cnae.descricao
            ))
            FROM unnest(string_to_array(NULLIF(est.cnae_fiscal_secundaria, ''), ',')) AS cod_cnae
            LEFT JOIN public.cnaes cnae ON cnae.codigo = cod_cnae
        ), '[]'::jsonb)
    ) AS "data_json"
FROM public.estabelecimentos est
LEFT JOIN public.empresas emp ON emp.cnpj_basico = est.cnpj_basico
LEFT JOIN public.naturezas_juridicas nat ON nat.codigo = emp.natureza_juridica
LEFT JOIN public.dados_simples sim ON sim.cnpj_basico = est.cnpj_basico
LEFT JOIN public.municipios mun ON mun.codigo = est.municipio
LEFT JOIN public.cnaes cnaep ON cnaep.codigo = est.cnae_fiscal_principal
WHERE est.cnpj_basico = %s AND est.cnpj_ordem = %s AND est.cnpj_dv = %s
LIMIT 1;
"""