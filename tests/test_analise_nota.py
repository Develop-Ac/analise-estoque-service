# -*- coding: utf-8 -*-
"""Análise de compra por nota (analise_nota.py) — leitura do XML e rateio do desembolso."""
import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import analise_nota as an  # noqa: E402

_COBR = """
<cobr>
  <fat><nFat>123</nFat><vOrig>1500.00</vOrig><vLiq>1500.00</vLiq></fat>
  <dup><nDup>001</nDup><dVenc>2026-12-01</dVenc><vDup>500.00</vDup></dup>
  <dup><nDup>002</nDup><dVenc>2026-12-15</dVenc><vDup>500.00</vDup></dup>
  <dup><nDup>003</nDup><dVenc>2027-01-05</dVenc><vDup>500.00</vDup></dup>
</cobr>"""

# Item 1: 1000 + frete 20 − desc 10 + IPI 50 + ST 80 + FCP-ST 10 = 1150
# Item 2: 300 + frete 5 + seguro 3 + outro 2 + IPI 15 + ST 25 = 350
_NFE = """
<NFe xmlns="http://www.portalfiscal.inf.br/nfe">
 <infNFe Id="NFe5126..." versao="4.00">
  <ide><nNF>4567</nNF><dhEmi>2026-11-10T14:30:00-04:00</dhEmi></ide>
  <emit><CNPJ>12345678000199</CNPJ><xNome>FORNECEDOR TESTE LTDA</xNome></emit>
  <det nItem="1">
   <prod><cProd>ABC-1</cProd><xProd>PARA-BRISA X</xProd><CFOP>6403</CFOP><uCom>UN</uCom>
    <qCom>10.0000</qCom><vProd>1000.00</vProd><vFrete>20.00</vFrete><vDesc>10.00</vDesc></prod>
   <imposto>
    <ICMS><ICMS10><vICMSST>80.00</vICMSST><vFCPST>10.00</vFCPST></ICMS10></ICMS>
    <IPI><IPITrib><vIPI>50.00</vIPI></IPITrib></IPI>
   </imposto>
  </det>
  <det nItem="2">
   <prod><cProd>XYZ-2</cProd><xProd>BORRACHA Y</xProd><CFOP>6102</CFOP><uCom>CX</uCom>
    <qCom>3.0000</qCom><vProd>300.00</vProd><vFrete>5.00</vFrete><vSeg>3.00</vSeg><vOutro>2.00</vOutro></prod>
   <imposto>
    <ICMS><ICMS10><vICMSST>25.00</vICMSST></ICMS10></ICMS>
    <IPI><IPITrib><vIPI>15.00</vIPI></IPITrib></IPI>
   </imposto>
  </det>
  <total><ICMSTot><vNF>{vnf}</vNF></ICMSTot></total>
  {cobr}
 </infNFe>
</NFe>"""


def _nfe(vnf="1500.00", cobr=_COBR):
    return _NFE.replace("{vnf}", vnf).replace("{cobr}", cobr)


def _proc(nfe):
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            '<nfeProc xmlns="http://www.portalfiscal.inf.br/nfe" versao="4.00">'
            + nfe + "<protNFe/><infNFe><det><prod/></det></infNFe></nfeProc>")  # rabo vazio visto em produção


def test_le_com_e_sem_nfeproc():
    for xml in (_nfe(), _proc(_nfe())):
        n = an.ler_xml(xml)
        assert n["cabecalho"] == {"emitente": "FORNECEDOR TESTE LTDA", "cnpj": "12345678000199", "numero": "4567",
                                  "emissao": datetime.date(2026, 11, 10), "valor_total": 1500.0}
        i1, i2 = n["itens"]
        assert (i1["n_item"], i1["cod_fornecedor"], i1["descricao"], i1["unidade"], i1["quantidade"], i1["cfop"]) == \
            (1, "ABC-1", "PARA-BRISA X", "UN", 10.0, "6403")
        assert i1["desembolso"] == 1150.0
        assert i2["desembolso"] == 350.0
        assert n["duplicatas"] == [
            {"numero": "001", "vencimento": datetime.date(2026, 12, 1), "valor": 500.0},
            {"numero": "002", "vencimento": datetime.date(2026, 12, 15), "valor": 500.0},
            {"numero": "003", "vencimento": datetime.date(2027, 1, 5), "valor": 500.0},
        ]


def test_sem_cobr_lista_vazia():
    assert an.ler_xml(_proc(_nfe(cobr="")))["duplicatas"] == []


def test_dup_sem_dvenc_fica_fora():
    cobr = "<cobr><dup><nDup>001</nDup><vDup>500.00</vDup></dup>" \
           "<dup><nDup>002</nDup><dVenc>2026-12-15</dVenc><vDup>1000.00</vDup></dup></cobr>"
    assert an.ler_xml(_nfe(cobr=cobr))["duplicatas"] == [
        {"numero": "002", "vencimento": datetime.date(2026, 12, 15), "valor": 1000.0}]


def test_nao_nfe_completa_levanta_valueerror():
    res = '<resNFe xmlns="http://www.portalfiscal.inf.br/nfe"><chNFe>1</chNFe><vNF>10.00</vNF></resNFe>'
    for xml in (res, "<NFe><ide>"):
        try:
            an.ler_xml(xml)
        except ValueError:
            continue
        raise AssertionError("esperava ValueError")


def test_rateio_fecha_com_vnf():
    # Soma dos itens 1500; vNF com desconto só no total (1450,01) → diferença rateada.
    n = an.ler_xml(_nfe(vnf="1450.01"))
    itens = an.ratear_desembolso(n["itens"], n["cabecalho"]["valor_total"])
    assert round(sum(round(i["desembolso"] * 100) for i in itens)) == 145001
    assert itens[0]["desembolso"] > 1100 and itens[1]["desembolso"] < 350
    # acréscimo e soma que já fecha
    assert sum(round(i["desembolso"] * 100) for i in an.ratear_desembolso(an.ler_xml(_nfe(vnf="1600.00"))["itens"], 1600.0)) == 160000
    intactos = an.ratear_desembolso(an.ler_xml(_nfe())["itens"], 1500.0)
    assert [i["desembolso"] for i in intactos] == [1150.0, 350.0]


# ---- montar_unidades -------------------------------------------------------------

def _rod(pro, **kw):
    base = {"descricao": f"PROD {pro}", "grupo_chave": None, "grupo_qtd_itens": 1,
            "grupo_estoque_max": None, "grupo_demanda_dia": None,
            "estoque_max_sugerido": None, "demanda_real_dia": None, "sob_encomenda": False}
    base.update(kw)
    return base


_ITEM = {"pro_codigo": "1", "descricao": "X", "quantidade": 8, "desembolso": 800.0}


def test_unidade_criterio_1_nao_lancada():
    rod = {"1": _rod("1", estoque_max_sugerido=12, demanda_real_dia=0.5)}
    [u] = an.montar_unidades([dict(_ITEM)], rod, {"1": 10}, lancada=False)
    assert (u["tipo"], u["estoque_antes"], u["estoque_depois"]) == ("produto", 10, 18)
    assert (u["cobertura_depois"], u["sai_de"], u["sai_ate"]) == (36.0, 20.0, 36.0)
    assert (u["excesso_existente"], u["excesso_gerado_qtd"], u["excesso_gerado_valor"]) == (0, 6, 600.0)
    assert u["selos"] == [] and u["valor_nota"] == 800.0


def test_unidade_criterio_2_lancada():
    rod = {"1": _rod("1", estoque_max_sugerido=12, demanda_real_dia=0.5)}
    [u] = an.montar_unidades([dict(_ITEM)], rod, {"1": 18}, lancada=True)
    assert (u["estoque_antes"], u["estoque_depois"], u["cobertura_depois"]) == (10, 18, 36.0)
    assert (u["sai_de"], u["sai_ate"]) == (20.0, 36.0)
    assert (u["excesso_gerado_qtd"], u["excesso_gerado_valor"]) == (6, 600.0)


def test_unidade_criterio_4_excesso_limitado_a_nota():
    rod = {"1": _rod("1", estoque_max_sugerido=12, demanda_real_dia=1)}
    [u] = an.montar_unidades([dict(_ITEM)], rod, {"1": 15}, lancada=False)
    assert (u["excesso_existente"], u["excesso_gerado_qtd"], u["excesso_gerado_valor"]) == (3, 8, 800.0)


def test_unidade_criterio_5_grupo_soma_marcas_e_membros():
    g = dict(grupo_chave="G1", grupo_qtd_itens=3, grupo_estoque_max=20, grupo_demanda_dia=1,
             estoque_max_sugerido=5, demanda_real_dia=0.1)
    rod = {"1": _rod("1", **g), "2": _rod("2", **g), "3": _rod("3", **g)}
    itens = [{"pro_codigo": "1", "descricao": "A", "quantidade": 4, "desembolso": 400.0},
             {"pro_codigo": "2", "descricao": "B", "quantidade": 6, "desembolso": 900.0}]
    [u] = an.montar_unidades(itens, rod, {"1": 5, "2": 5, "3": 5}, lancada=False)
    assert (u["tipo"], u["chave"], u["qtd_nota"], u["valor_nota"]) == ("grupo", "G1", 10, 1300.0)
    assert [p["pro_codigo"] for p in u["produtos"]] == ["1", "2"]
    assert (u["estoque_antes"], u["estoque_depois"], u["maximo"], u["venda_dia"]) == (15, 25, 20, 1)
    assert (u["excesso_gerado_qtd"], u["excesso_gerado_valor"]) == (5, 650.0)


def test_unidade_criterio_6_produto_sozinho_usa_dados_do_produto():
    rod = {"1": _rod("1", grupo_chave="G9", grupo_qtd_itens=1, grupo_estoque_max=100,
                     grupo_demanda_dia=9, estoque_max_sugerido=12, demanda_real_dia=0.5)}
    [u] = an.montar_unidades([dict(_ITEM)], rod, {"1": 10}, lancada=False)
    assert (u["tipo"], u["maximo"], u["venda_dia"], u["excesso_gerado_qtd"]) == ("produto", 12, 0.5, 6)


def test_unidade_criterio_7_sem_maximo_selos():
    rod = {"1": _rod("1", sob_encomenda=True)}
    itens = [dict(_ITEM), {"pro_codigo": "2", "descricao": "FORA", "quantidade": 1, "desembolso": 50.0}]
    us = {u["chave"]: u for u in an.montar_unidades(itens, rod, {"1": 10}, lancada=False)}
    assert us["1"]["selos"] == ["sob_encomenda", "sem_giro"]
    assert us["1"]["excesso_gerado_qtd"] == 0 and us["1"]["excesso_gerado_valor"] == 0
    assert us["1"]["cobertura_depois"] is None and us["1"]["sai_de"] is None
    assert us["2"]["selos"] == ["produto_novo", "sem_giro"] and us["2"]["rotulo"] == "FORA"


def test_unidade_sem_giro_com_maximo_e_saldo_negativo_zera():
    g = dict(grupo_chave="G1", grupo_qtd_itens=2, grupo_estoque_max=3, grupo_demanda_dia=0)
    rod = {"1": _rod("1", **g), "2": _rod("2", **g)}
    [u] = an.montar_unidades([dict(_ITEM)], rod, {"1": -5, "2": 2}, lancada=False)
    assert (u["estoque_antes"], u["estoque_depois"]) == (2, 10)
    assert u["selos"] == ["sem_giro"] and u["excesso_gerado_qtd"] == 7 and u["sai_ate"] is None


def test_unidades_ordenadas_por_excesso():
    rod = {"1": _rod("1", estoque_max_sugerido=0, demanda_real_dia=1),
           "2": _rod("2", estoque_max_sugerido=0, demanda_real_dia=1)}
    itens = [{"pro_codigo": "1", "descricao": "A", "quantidade": 1, "desembolso": 10.0},
             {"pro_codigo": "2", "descricao": "B", "quantidade": 1, "desembolso": 90.0}]
    assert [u["chave"] for u in an.montar_unidades(itens, rod, {}, lancada=False)] == ["2", "1"]


# ---- custo de caixa e resumo ----------------------------------------------------

def _u_criterio_1():
    rod = {"1": _rod("1", estoque_max_sugerido=12, demanda_real_dia=0.5)}
    return an.montar_unidades([dict(_ITEM)], rod, {"1": 10}, lancada=False)


def test_custo_caixa_criterio_3():
    us = _u_criterio_1()
    cc = an.aplicar_custo_caixa(us, 14, 0.0217, 365)
    assert (us[0]["custo_caixa_compra"], us[0]["custo_caixa_excesso"]) == (8.10, 6.94)
    assert (cc["custo_compra"], cc["custo_excesso"], cc["prazo_medio_dias"]) == (8.10, 6.94, 14)
    assert (cc["taxa_mes"], cc["dias_teto"]) == (0.0217, 365)


def test_prazo_medio_ponderado_e_vazio():
    base = datetime.date(2026, 11, 1)
    venc = [{"vencimento": datetime.date(2026, 11, 11), "valor": 100.0},   # 10 dias
            {"vencimento": datetime.date(2026, 12, 1), "valor": 300.0},    # 30 dias
            {"vencimento": datetime.date(2026, 10, 20), "valor": 600.0}]   # vencido = 0
    assert an.prazo_medio(venc, base) == 10.0   # (1000 + 9000 + 0) / 1000
    assert an.prazo_medio([], base) == 0.0


def test_custo_caixa_sem_giro_no_teto_com_aviso():
    rod = {"1": _rod("1", estoque_max_sugerido=12, demanda_real_dia=0)}
    us = an.montar_unidades([dict(_ITEM)], rod, {"1": 10}, lancada=False)
    an.aplicar_custo_caixa(us, 0, 0.0217, 365)
    assert us[0]["custo_caixa_compra"] == round(800 * 0.0217 * 365 / 30, 2)
    assert us[0]["avisos"] == ["sem giro: simulado em 12 meses"]


def test_resumo_criterio_8_com_nao_analisado():
    us = _u_criterio_1()
    an.aplicar_custo_caixa(us, 14, 0.0217, 365)
    nao = [{"n_item": 2, "descricao": "CADEIRA", "valor": 200.0, "motivo": "imobilizado ou uso e consumo"}]
    r = an.montar_resumo(1000.0, us, nao)
    assert (r["valor_nota"], r["valor_analisado"], r["perc_analisado"]) == (1000.0, 800.0, 80.0)
    assert (r["excesso_gerado_valor"], r["excesso_gerado_perc"]) == (600.0, 60.0)
    assert (r["custo_caixa_compra"], r["custo_caixa_excesso"]) == (8.10, 6.94)
    assert (r["valor_produto_novo"], r["valor_sob_encomenda"]) == (0, 0)


# --- Fluxo a pagar -------------------------------------------------------------------------

_D = datetime.date
_QUA = _D(2026, 11, 11)  # quarta-feira


def test_tres_duplicatas_semanas_certas():
    venc = [{"numero": n, "vencimento": d, "valor": 500.0, "pago_em": None}
            for n, d in (("001", _D(2026, 11, 13)), ("002", _D(2026, 11, 25)), ("003", _D(2026, 12, 9)))]
    f = an.montar_fluxo(venc, {_D(2026, 11, 12): 100.0, _D(2026, 11, 24): 200.0}, _QUA)
    assert len(f["vencimentos"]) == 3 and f["avisos"] == []
    s = f["semanas"]
    assert [x["inicio"] for x in s] == [_D(2026, 11, 9), _D(2026, 11, 16), _D(2026, 11, 23),
                                        _D(2026, 11, 30), _D(2026, 12, 7)]
    assert s[-1]["fim"] == _D(2026, 12, 13)
    assert (s[0]["ja_comprometido"], s[0]["esta_nota"], s[0]["novo_total"], s[0]["da_nota"]) == (100, 500, 600, True)
    assert (s[2]["ja_comprometido"], s[2]["esta_nota"], s[2]["novo_total"]) == (200, 500, 700)
    assert s[4]["esta_nota"] == 500 and s[1]["da_nota"] is False and s[1]["novo_total"] == 0


def test_titulo_pago_fica_na_lista_e_fora_do_quadro():
    venc = [{"numero": "1", "vencimento": _D(2026, 11, 20), "valor": 300.0, "pago_em": _D(2026, 11, 10)},
            {"numero": "2", "vencimento": _D(2026, 11, 27), "valor": 400.0, "pago_em": None}]
    f = an.montar_fluxo(venc, {}, _QUA)
    assert f["vencimentos"][0]["pago_em"] == _D(2026, 11, 10)
    assert [x["esta_nota"] for x in f["semanas"]] == [0, 0, 400]


def test_comprometido_vencido_nao_entra():
    f = an.montar_fluxo([], {_D(2026, 11, 10): 999.0, _D(2026, 11, 11): 50.0}, _QUA)
    assert sum(x["ja_comprometido"] for x in f["semanas"]) == 50


def test_sem_duplicata_aviso_e_oito_semanas():
    f = an.montar_fluxo([], {_D(2026, 12, 1): 80.0}, _QUA)
    assert f["avisos"] == [an.AVISO_SEM_VENCIMENTO] and f["vencimentos"] == []
    assert len(f["semanas"]) == 8 and all(x["esta_nota"] == 0 for x in f["semanas"])
    assert sum(x["ja_comprometido"] for x in f["semanas"]) == 80


def test_semana_comeca_na_segunda():
    assert an.janela_semanas([], _QUA)[0] == _D(2026, 11, 9)
    ini, fim = an.janela_semanas([], _D(2026, 11, 15))  # domingo
    assert ini == _D(2026, 11, 9) and fim == _D(2027, 1, 3) and fim.weekday() == 6


def test_hoje_cuiaba_e_data():
    assert isinstance(an.hoje_cuiaba(), datetime.date)


# ---------------------------------------------------------------- ponta a ponta
_NFE_E2E = """<nfeProc xmlns="http://www.portalfiscal.inf.br/nfe"><NFe><infNFe>
 <ide><nNF>9</nNF><dhEmi>2026-11-03T10:00:00-04:00</dhEmi></ide>
 <emit><CNPJ>1</CNPJ><xNome>F</xNome></emit>
 <det nItem="1"><prod><cProd>A</cProd><xProd>PARA-BRISA</xProd><qCom>8</qCom><vProd>800.00</vProd></prod></det>
 <det nItem="2"><prod><cProd>B</cProd><xProd>CADEIRA</xProd><qCom>1</qCom><vProd>200.00</vProd></prod></det>
 <total><ICMSTot><vNF>1000.00</vNF></ICMSTot></total>
 <cobr><dup><nDup>001</nDup><dVenc>2026-11-25</dVenc><vDup>1000.00</vDup></dup></cobr>
</infNFe></NFe></nfeProc>"""


def _e2e(conc):
    n = an.ler_xml(_NFE_E2E)
    itens = an.ratear_desembolso(n["itens"], n["cabecalho"]["valor_total"])
    ok, nao = an.resolver_itens(itens, {}, conc, {}, {}, {"1": {"COMERCIALIZAVEL": "S"}}, False)
    venc = [{**d, "pago_em": None} for d in n["duplicatas"]]
    rod = {"1": _rod("1", estoque_max_sugerido=12, demanda_real_dia=0.5)}
    return an.montar_analise(n["cabecalho"]["valor_total"], ok, nao, rod, {"1": 10}, False,
                             venc, _QUA, "hoje", {}, _QUA, 0.0217, 365)


def test_ponta_a_ponta_criterios_1_e_3():
    r = _e2e({1: "1"})   # item 2 sem produto -> não analisado
    [u] = r["unidades"]
    assert (u["estoque_antes"], u["estoque_depois"], u["cobertura_depois"]) == (10, 18, 36.0)
    assert (u["excesso_gerado_qtd"], u["excesso_gerado_valor"]) == (6, 600.0)
    assert r["custo_caixa"]["prazo_medio_dias"] == 14.0 and r["custo_caixa"]["base_prazo"] == "hoje"
    assert (r["resumo"]["custo_caixa_compra"], r["resumo"]["custo_caixa_excesso"]) == (8.10, 6.94)
    assert r["resumo"]["valor_analisado"] + sum(x["valor"] for x in r["nao_analisados"]) == r["resumo"]["valor_nota"]
    assert r["fluxo"]["vencimentos"][0]["valor"] == 1000.0


def test_ponta_a_ponta_sem_item_analisavel():
    r = _e2e({})
    assert r["unidades"] == [] and len(r["nao_analisados"]) == 2
    assert (r["resumo"]["valor_analisado"], r["resumo"]["perc_analisado"]) == (0, 0)
    assert r["custo_caixa"]["custo_compra"] == 0 and len(r["fluxo"]["semanas"]) >= 1


# ---------------------------------------------------------------- resolver_itens
def _xml_item(n=1, cprod="00ABC", qtd=2.0, valor=100.0):
    return {"n_item": n, "cod_fornecedor": cprod, "descricao": f"ITEM {n}", "quantidade": qtd, "desembolso": valor}


_CAD = {str(p): {"COMERCIALIZAVEL": "S", "PRO_DESCRICAO": f"PROD {p}"} for p in (10, 20, 30, 40, 41)}


def _resolver(itens, entrada=None, conc=None, vinc=None, pf=None, cad=_CAD, lancada=False):
    return an.resolver_itens(itens, entrada or {}, conc or {}, vinc or {}, pf or {}, cad, lancada)


def test_precedencia_dos_quatro_passos():
    entrada = {1: {"PRO_CODIGO": 10, "QUANTIDADE": 24, "CFOP_NOTA": "1403", "FATOR_CONVERSAO": 12}}
    conc, vinc, pf = {1: "20", 2: "20"}, {"ABC": 30}, {"ABC": [{"PRO_CODIGO": 40, "FATOR_CONVERSAO": 1}]}
    item = [_xml_item()]
    ok, _ = _resolver(item, entrada, conc, vinc, pf, lancada=True)
    assert (ok[0]["pro_codigo"], ok[0]["quantidade"]) == ("10", 24.0)
    assert _resolver(item, entrada, conc, vinc, pf)[0][0]["pro_codigo"] == "20"  # não lançada ignora a entrada
    assert _resolver(item, conc={2: "20"}, vinc=vinc, pf=pf)[0][0]["pro_codigo"] == "30"
    assert _resolver(item, pf=pf)[0][0]["pro_codigo"] == "40"


def test_prod_fornecedor_com_dois_produtos_fica_com_o_do_cadastro():
    pf = {"ABC": [{"PRO_CODIGO": 99, "FATOR_CONVERSAO": 1}, {"PRO_CODIGO": 41, "FATOR_CONVERSAO": 1}]}
    assert _resolver([_xml_item()], pf=pf)[0][0]["pro_codigo"] == "41"


def test_item_sem_produto_vai_para_nao_analisados():
    ok, nao = _resolver([_xml_item(valor=55.5)])
    assert ok == []
    assert nao == [{"n_item": 1, "descricao": "ITEM 1", "valor": 55.5, "motivo": an.MOTIVO_SEM_PRODUTO}]


def test_nao_comercializavel_vai_para_nao_analisados():
    cad = {**_CAD, "20": {"COMERCIALIZAVEL": "N"}}
    ok, nao = _resolver([_xml_item()], conc={1: "20"}, cad=cad)
    assert ok == [] and nao[0]["motivo"] == an.MOTIVO_IMOBILIZADO


def test_cfop_de_imobilizado_e_consumo_na_nota_lancada():
    entrada = {1: {"PRO_CODIGO": 10, "QUANTIDADE": 1, "CFOP_NOTA": "1556"},
               2: {"PRO_CODIGO": 10, "QUANTIDADE": 1, "CFOP_NOTA": "2551"},
               3: {"PRO_CODIGO": 10, "QUANTIDADE": 1, "CFOP_NOTA": "1403"}}
    ok, nao = _resolver([_xml_item(1), _xml_item(2), _xml_item(3)], entrada, lancada=True)
    assert [x["n_item"] for x in nao] == [1, 2] and {x["motivo"] for x in nao} == {an.MOTIVO_IMOBILIZADO}
    assert [x["n_item"] for x in ok] == [3]


def test_fator_na_nao_lancada_e_aviso_sem_fator():
    pf = {"ABC": [{"PRO_CODIGO": 20, "FATOR_CONVERSAO": 12}]}
    ok, _ = _resolver([_xml_item(qtd=3)], conc={1: "20"}, pf=pf)  # fator do par mesmo vindo da conciliação
    assert ok[0]["quantidade"] == 36.0 and "aviso" not in ok[0]
    ok, _ = _resolver([_xml_item(qtd=3)], conc={1: "20"})
    assert ok[0]["quantidade"] == 3.0 and ok[0]["aviso"] == an.AVISO_SEM_FATOR


def test_cprod_com_zeros_a_esquerda_casa():
    assert an.norm_cprod(" 000ab 12 ") == "AB12" and an.norm_cprod("000") == "0" and an.norm_cprod(None) == ""
    ok, _ = _resolver([_xml_item(cprod="0000ABC")], vinc={"ABC": 30})
    assert ok[0]["pro_codigo"] == "30"
