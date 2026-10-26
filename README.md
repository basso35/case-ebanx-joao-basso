# OrionPay · Financial Close & Performance

Case de Senior Analytics & Intelligence. O projeto trata os dados da OrionPay, monta uma P&L gerencial em USD com visão oficial e visão atualizada e responde às cinco perguntas de negócio.

## Como rodar

```bash
pip install -r requirements.txt     # PySpark 4 exige Java 17 ou superior
```

1. Coloque `OrionPay_Case_Senior.xlsx` na raiz do projeto.
2. Rode `01_tratamento_dados.ipynb`. Ele gera `dados/bronze`, `dados/silver` e `dados/gold`.
3. Rode `02_analise.ipynb`. Ele lê a camada gold e salva os gráficos em `graficos/`.

## Estrutura

```
├── functions.py                 funções reutilizáveis (pipeline Spark, P&L, gráficos)
├── 01_tratamento_dados.ipynb    bronze → silver → gold em Spark
├── 02_analise.ipynb             P&L, perguntas de negócio e gráficos
├── dados/
│   ├── bronze/                  abas do Excel como vieram, B01 e B02 empilhados
│   ├── silver/                  dados tipados, limpos, convertidos e classificados
│   └── gold/                    fatos, P&L mensal, log de qualidade e conferências
└── graficos/                    PNGs usados neste README
```

### `functions.py`

Concentra tudo o que é usado em mais de um lugar, para que os notebooks contenham só a lógica de cada etapa:

- **Leitura e escrita**: `read_excel_sheets`, `write_csv`, `read_csv`.
- **Tratamento** (uma função por regra do case): `drop_exact_duplicates`, `correct_fx_outliers`, `impute_currency_from_country`, `convert_to_usd` (converte e arredonda linha a linha em decimal), `classify_entries` (join por vigência `valid_from ≤ accounting_date < valid_to`), `flag_official_close` e `flag_tpv`.
- **Modelagem**: `build_fact_tpv`, `build_fact_pnl`, `monthly_pnl_spark`.
- **Análise**: `pnl_summary` calcula TPV, Net Revenue, Cost of Sales, GP, take rate, GP/TPV e margem para qualquer recorte, em qualquer visão (oficial ou atualizada) e com qualquer combinação de lotes.
- **Gráficos**: estilo único (`set_chart_style`), títulos com subtítulo e fonte (`chart_titles`), `waterfall` e formatadores.

### 01 · Tratamento de dados (Spark)

| Camada | O que acontece |
|---|---|
| **Bronze** | As abas de dados do Excel são lidas como texto e B01 e B02 são empilhados (B02 acumula sobre B01, não substitui). Nenhuma regra é aplicada. |
| **Silver** | Tipagem; remoção de duplicados exatos; correção do câmbio COP de mar/25; imputação de moeda; conversão para USD por linha; classificação contábil pela regra vigente; flags `is_official`, `is_late_posting`, `is_mapped` e `is_tpv`. Seis validações garantem unicidade de IDs, ausência de USD nulo, que a classificação não duplicou linhas e que toda captura confirmada tem taxa. |
| **Gold** | `fact_tpv` e `fact_pnl` (grão: mês × país × merchant × produto × método × conta × lote × flags); `pnl_monthly` nas duas visões; dimensões; `data_quality_log.csv`; `reconciliation_checks.csv`. |

**Visão oficial**: considera só os lançamentos com `posted_at ≤ official_cutoff_at` nos meses fechados. **Visão atualizada**: considera tudo o que chegou até B02 (20/09/2026).

### 02 · Análise

Começa com a P&L mensal oficial e a decomposição do take rate em efeito preço e efeito mix. Depois responde uma pergunta por seção, cada uma com números, gráfico, leitura e ressalvas.

## Problemas de dados

| ID | Problema | Registros | Tratamento | Impacto (USD) |
|---|---|---:|---|---:|
| DQ01 | Lançamentos duplicados (mesmo `entry_id` e campos idênticos), todos em jul/26 | 427 | Removidos | GP de jul/26 +181 se mantidos |
| DQ02 | Câmbio COP de mar/25 = 0,002462, 10× a série (casa decimal deslocada) | 672 | Taxa ÷ 10, original preservada | TPV mar/25 −250k; GP −3,5k |
| DQ03 | Lançamentos sem `currency_code` | 170 | Moeda imputada pelo país (100% de aderência nos demais) | 1,5k de receita recuperada |
| DQ04 | Conta Z900 (compliance) sem regra no `account_mapping` | 6 | Fora do GP, reportada como `unmapped` | −3,6k (jun–ago/26) |
| DQ05 | C190 com competência 28/08, postado em 18/09 (após o cutoff de 05/09) | 1 | Só na visão atualizada | GP ago/26 −2,4k |
| DQ06 | C120 passa de custo direto para indireto em jan/26 | 7.156 | Regra vigente aplicada; série pró-forma comparável | −3,5k reclassificados; GP inalterado |
| DQ07 | Lançamentos sem merchant (C200, R120, C190) | 102 | `UNATTRIBUTED`, fora do ranking de merchants | −27,4k |
| DQ08 | Eventos REFUND, PENDING e CANCELLED | 1.418 | Fora do TPV conforme regra | 352,8k fora do TPV |
| DQ09 | Incentivo contratual R120 positivo (mai/26, MX, sem merchant) | 1 | Mantido, destacado como pontual; sinal a confirmar | +3,0k |
| DQ10 | Resultado de FX (C300) positivo em todos os lançamentos de mar/26 | 123 | Mantido (a regra permite custo positivo), destacado como pontual | +374 |
| DQ11 | Set/26 aberto e parcial (até 20/09) | 3.204 | Sem visão oficial; comparado só like-for-like | – |

## Conferências

Todas as conferências passam (`dados/gold/reconciliation_checks.csv`):

- **Linhas preservadas**: bronze → silver, descontando os duplicados removidos.
- **Totais preservados**: USD silver = USD `fact_pnl`, e TPV silver = TPV `fact_tpv`.
- **Composição da P&L**: GP = Net Revenue + Cost of Sales nos 24 meses.
- **Visões**: Atualizada − Oficial nos meses fechados = soma das postagens tardias (−2.400,00). Oficial B01 = Oficial B01+B02.
- **Recortes**: soma dos países = total, e soma dos merchants + não atribuído = total.

## Principais insights

![Spread](graficos/00_spread_unitario.png)

**O volume cresce, mas o spread foi a zero.** Em dois anos, a receita líquida por dólar processado caiu de 3,62% para 2,77% e o custo de venda por dólar subiu de 1,98% para 2,80%. O GP de ago/26 é de −US$ 69. Da queda de 0,84 p.p. no take rate, 0,54 p.p. vem de preço (dentro de cada país, sobretudo no Brasil) e 0,30 p.p. de mix (o Brasil foi de 32% para 74% do TPV).

### P1 · Mês com maior Gross Profit

![GP mensal](graficos/01_gp_mensal.png)

Cada leitura leva a uma resposta diferente:

- **Dados crus**: mar/25 (US$ 5,7k). É um artefato do câmbio COP errado.
- **Visão oficial**: mai/26 (US$ 4,0k). Mas US$ 3,0k vêm de um único incentivo contratual (R120); sem ele, o GP do mês seria US$ 989.
- **Melhor mês recorrente**: nov/24 (US$ 2,8k). Teve 25% mais pagamentos com ticket estável e crescimento em todos os países.

A hipótese de sazonalidade de fim de ano é plausível, mas não está provada: o volume sobe ao longo de todo o mês, sem pico na Black Friday.

![Pontes](graficos/01b_pontes_gp.png)

### P2 · Ago/26: países e merchants

![Países](graficos/02_paises_ago26.png)

- **País líder**: o Brasil lidera o Net Revenue com US$ 5,0k (65% do total), o que equivale a 2,44% do TPV brasileiro, o menor take rate entre os países. Mesmo assim, o GP do Brasil é negativo.
- **Merchant líder**: o maior GP atribuído é do Orion Horizonte (M6205), com US$ 94 somando os 4 países. Ele é só o 11º em volume.
- **Volume sem rentabilidade**: os 5 maiores merchants têm 38% do TPV e 10% do GP atribuído.
- **Valores não atribuíveis**: o shared services (−US$ 1.548) entra no GP total, mas não no de nenhum merchant. A Z900 (−US$ 1.200) fica fora do GP.

![Merchants](graficos/02b_merchants_volume_vs_rentabilidade.png)

### P3 · Custos diretos por produto

![Custos](graficos/03_custos_diretos.png)

- **Distribuição**: PAY_IN responde por cerca de 63% do custo direto.
- **Crescimento**: o custo de PAY_OUT triplicou em 12 meses (+212%, contra +50% de PAY_IN).
- **Custo por dólar**: na base comparável, subiu cerca de 55% nos dois produtos.
- **Comparabilidade**: a série como reportada não é comparável, porque o C120 vira custo indireto em jan/26.
- **Sem produto**: C190 (só na visão atualizada), C200 e Z900.

### P4 · Jan–set, 2026 contra 2025

![YoY](graficos/04_yoy_jan_set.png)

- **Jan–ago**: TPV +44,5% e GP −34,5% (−52% sem o incentivo pontual).
- **Set, dias 1–20**: TPV +43,6% e GP transacional −55%.

### P5 · O que mudou com B02

![B02](graficos/05_impacto_b02.png)

- **Visão oficial**: o B02 não altera nenhum número oficial, e ela não deve mudar.
- **Ago/26 atualizado**: cai para −US$ 2.469, por causa da fatura C190 que chegou depois do cutoff.
- **Set/26**: ganha +US$ 48,7k de TPV, e o GP parcial passa de −US$ 93 para +US$ 92.

**Recomendação de publicação**:

- **Para Finance**: publicar a P&L oficial.
- **Para leitura de performance**: publicar a visão atualizada com ressalva.
- **Set/26**: tratar como parcial.

**Ações priorizadas**:

1. Rever o pricing do Brasil e de PAY_OUT.
2. Fechar a governança contábil: mapear a Z900, confirmar o sinal do R120, corrigir o câmbio na fonte, controlar cutoff de faturas e garantir unicidade de `entry_id`.
3. Renegociar o custo de processamento (C100) e rever o rateio de shared services (C200).

## Decisões e limitações

- **Câmbio COP**: a correção é uma hipótese forte, porque o valor ÷ 10 encaixa exatamente na série. Mesmo assim, precisa ser confirmada com Tesouraria.
- **Z900**: ficou fora do GP por falta de regra. Se for custo de venda, o GP de jun–ago/26 cai US$ 1,2k por mês.
- **R120**: o sinal positivo numa conta de ajuste de receita precisa ser validado com Finance.
- **Atribuição a merchants**: considera todos os custos com merchant, inclusive o C120 depois da reclassificação. O shared services não é rateado.
- **Fora do escopo**: não há dados de contrato, custo de aquisição ou fornecedor. As causas da queda de preço e da alta de custo ficam como hipóteses.

## Uso de IA

O projeto foi desenvolvido com o Claude como par de programação, para explorar os dados, escrever o pipeline e gerar os gráficos. Os números foram validados por conferências independentes:

- reconciliação entre camadas;
- recálculo em pandas dos totais calculados em Spark;
- checagem manual de casos como o câmbio de mar/25 e os duplicados.

Os textos dos notebooks foram revisados contra as saídas executadas.

## Executive summary

OrionPay processed US$ 5.2M in confirmed TPV between Sep/24 and Sep 20, 2026, and volume grew 44% year over year (Jan–Aug). Profitability went the opposite way. Net revenue per dollar processed fell from 3.62% to 2.77%, while cost of sales per dollar rose from 1.98% to 2.80%. As a result, monthly Gross Profit went from US$ 2.1k to −US$ 69 in Aug/26 (official view), and Jan–Aug GP is down 34% year over year (−52% excluding a one-off incentive).

The drivers are mix and price. Brazil, the lowest-take-rate market, grew from 32% to 74% of TPV, and its take rate dropped from 3.13% to 2.44%. PAY_OUT, which carries a lower take rate and higher costs, went from 14% to 44% of TPV. Brazil leads net revenue but is GP-negative, and the five largest merchants hold 38% of TPV but only 10% of attributed GP.

Several data issues were found and fixed: a 10× COP FX rate in Mar/25, 427 duplicated entries, 170 entries with no currency, an unmapped compliance account (Z900), and a supplier invoice posted after the August cutoff. Without the FX fix, Mar/25 would wrongly appear as the best month. The highest official GP month is May/26, but 75% of it comes from a single contractual incentive; the best recurring month is Nov/24.

Batch B02 does not change any official close. It lowers the updated Aug/26 GP to −US$ 2.5k because of the late invoice, and it adds US$ 48.7k of TPV to September, which is still open.

We recommend three actions: (1) reprice Brazil and PAY_OUT; (2) close the accounting gaps (map Z900, confirm the sign of the incentive, enforce cutoff and uniqueness controls); and (3) renegotiate processing costs and review the shared-services allocation.
