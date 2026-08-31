# -*- coding: utf-8 -*-
"""Rotas da sugestao de compra: /compras/sugestao e /compras/sugestao/pdf."""
import io
import math
import os
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from typing import List, Optional
from sqlalchemy import text
from sugestao_compra import _gerar_sugestao_compra, _render_sugestao_pdf

router = APIRouter()

@router.get("/compras/sugestao")
def sugestao_compra(
    fornecedor: Optional[str] = None,
    curva: Optional[str] = None,
    subgrupo: Optional[str] = None,
    marca: Optional[str] = None,
    apenas_zerados: bool = False,
    usar_estoque_realtime: bool = True,
    consolidar_grupo: bool = True,
    incluir_sem_historico: bool = False,
):
    """
    Lista o que COMPRAR, agrupado por fornecedor, usando o ponto de pedido.

      Posição   = estoque atual (ERP) + em trânsito (pedidos Liberado / Em Trânsito parcialmente,
                  já descontado o que foi recebido por NF)
      Comprar?  = Posição <= ponto de pedido
      Quanto?   = Máximo - Posição

    Com consolidar_grupo=True (padrão): usa o mín/máx CONSOLIDADO do grupo (mesma descrição,
    várias marcas), soma a posição de todas as marcas e devolve 1 linha por grupo; produtos
    "Sob Encomenda" (originais) são omitidos. Com False: usa o mín/máx individual por marca.
    """
    return _gerar_sugestao_compra(
        fornecedor=fornecedor, curva=curva, subgrupo=subgrupo, marca=marca,
        apenas_zerados=apenas_zerados, usar_estoque_realtime=usar_estoque_realtime,
        consolidar_grupo=consolidar_grupo, incluir_sem_historico=incluir_sem_historico,
    )


@router.get("/compras/sugestao/pdf")
def sugestao_compra_pdf(
    fornecedor: Optional[str] = None,
    curva: Optional[str] = None,
    subgrupo: Optional[str] = None,
    marca: Optional[str] = None,
    apenas_zerados: bool = False,
    usar_estoque_realtime: bool = True,
    consolidar_grupo: bool = True,
    incluir_sem_historico: bool = False,
):
    """
    Mesma sugestão de compra do endpoint /compras/sugestao, porém renderizada
    como um PDF (uma tabela por fornecedor). Aceita os mesmos filtros.
    """
    data = _gerar_sugestao_compra(
        fornecedor=fornecedor, curva=curva, subgrupo=subgrupo, marca=marca,
        apenas_zerados=apenas_zerados, usar_estoque_realtime=usar_estoque_realtime,
        consolidar_grupo=consolidar_grupo, incluir_sem_historico=incluir_sem_historico,
    )
    pdf_bytes = _render_sugestao_pdf(
        data,
        filtros={"fornecedor": fornecedor, "curva": curva, "subgrupo": subgrupo,
                 "marca": marca, "apenas_zerados": apenas_zerados},
    )
    headers = {"Content-Disposition": 'inline; filename="sugestao_compra.pdf"'}
    return StreamingResponse(io.BytesIO(pdf_bytes), media_type="application/pdf",
                             headers=headers)


