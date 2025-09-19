import streamlit as st
import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from collections import defaultdict
from datetime import datetime
import requests
import io
from pypdf import PdfWriter, PdfReader

# --- Configuração do App e Conexão com Supabase ---
st.set_page_config(page_title="Painel de Comprovantes", layout="wide")
st.title("⚡️ Painel de Busca de Comprovantes")

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Função para inicializar a conexão com o Supabase (em cache)
@st.cache_resource
def init_supabase_client():
    """Cria e retorna o cliente Supabase, em cache para evitar reconexões."""
    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        st.error("As variáveis de ambiente SUPABASE_URL e SUPABASE_KEY não foram definidas. Crie o arquivo .env.")
        return None
    return create_client(url, key)

supabase = init_supabase_client()

# --- Busca e Processamento dos Dados ---
@st.cache_data(ttl=600)
def fetch_data_from_supabase(_db_client: Client):
    """Busca todos os comprovantes do banco de dados, paginando os resultados."""
    all_data = []
    offset = 0
    page_size = 1000  # Corresponde ao limite padrão do Supabase

    while True:
        try:
            response = _db_client.table('comprovantes').select(
                "nome_recebedor, cnpj_recebedor, chave_pix, data_transferencia, valor, pdf_url"
            ).range(offset, offset + page_size - 1).execute()

            if not response.data:
                break  # Sai do loop se não houver mais dados

            all_data.extend(response.data)
            offset += page_size

        except Exception as e:
            st.error(f"Erro ao buscar dados do Supabase: {e}")
            return all_data
            
    return all_data

# --- Interface Principal ---
if not supabase:
    st.stop()

all_receipts = fetch_data_from_supabase(supabase)

if not all_receipts:
    st.warning("Nenhum comprovante encontrado no banco de dados. Use o script 'upload_to_supabase.py' para enviar dados.")
else:
    search_term = st.text_input("Buscar por CNPJ/CPF, Nome ou Chave PIX:")

    if search_term:
        search_term_lower = search_term.lower()
        filtered_receipts = [
            r for r in all_receipts if
            (r.get('nome_recebedor') and search_term_lower in r['nome_recebedor'].lower()) or
            (r.get('cnpj_recebedor') and search_term_lower in r['cnpj_recebedor'].lower()) or
            (r.get('chave_pix') and search_term_lower in r['chave_pix'].lower())
        ]
    else:
        filtered_receipts = all_receipts

    if not filtered_receipts:
        st.info("Nenhum comprovante encontrado para o termo buscado.")
    else:
        mapping = defaultdict(lambda: defaultdict(list))
        cnpj_to_name = {}
        for receipt in filtered_receipts:
            cnpj = receipt['cnpj_recebedor']
            date_str = datetime.strptime(receipt['data_transferencia'], '%Y-%m-%d').strftime('%d/%m/%Y')
            
            if cnpj not in cnpj_to_name:
                cnpj_to_name[cnpj] = receipt['nome_recebedor']
            
            mapping[cnpj][date_str].append(receipt)

        display_options = {f"{cnpj} - {name}": cnpj for cnpj, name in cnpj_to_name.items()}
        
        if not display_options:
            st.info("Nenhum recebedor corresponde à busca.")
        else:
            selected_display = st.selectbox("Selecione o Recebedor", list(display_options.keys()))

            if selected_display:
                selected_cnpj = display_options[selected_display]
                
                all_date_receipts = mapping.get(selected_cnpj, {})
                all_receipts_for_cnpj = []
                sorted_dates = sorted(all_date_receipts.keys(), key=lambda d: datetime.strptime(d, '%d/%m/%Y'), reverse=True)
                
                for date_key in sorted_dates:
                    all_receipts_for_cnpj.extend(all_date_receipts[date_key])

                if all_receipts_for_cnpj:
                    # --- Botão para baixar todos os PDFs ---
                    if len(all_receipts_for_cnpj) > 1:
                        
                        @st.cache_data
                        def merge_pdfs_from_urls(_receipts_tuple):
                            """Busca, une e retorna os dados de um PDF combinado."""
                            receipts = [dict(item) for item in _receipts_tuple]
                            pdf_writer = PdfWriter()
                            
                            for r in receipts:
                                pdf_url = r.get('pdf_url')
                                if pdf_url:
                                    try:
                                        response = requests.get(pdf_url, timeout=10)
                                        response.raise_for_status()
                                        pdf_reader = PdfReader(io.BytesIO(response.content))
                                        for page in pdf_reader.pages:
                                            pdf_writer.add_page(page)
                                    except requests.exceptions.RequestException:
                                        pass # Ignora erros de download para não quebrar a UI
                            
                            merged_pdf_buffer = io.BytesIO()
                            pdf_writer.write(merged_pdf_buffer)
                            return merged_pdf_buffer.getvalue()

                        # Transforma a lista de dicionários em um tipo hasheável para o cache
                        hashable_receipts = tuple(frozenset(d.items()) for d in all_receipts_for_cnpj)
                        merged_pdf_data = merge_pdfs_from_urls(hashable_receipts)

                        st.download_button(
                            label=f"📥 Baixar todos os {len(all_receipts_for_cnpj)} comprovantes (PDF único)",
                            data=merged_pdf_data,
                            file_name=f"comprovantes_{selected_cnpj}.pdf",
                            mime="application/pdf"
                        )

                    st.write(f"### {len(all_receipts_for_cnpj)} Comprovante(s) encontrado(s):")
                    st.divider()

                    total_value = 0
                    
                    for i, item in enumerate(all_receipts_for_cnpj):
                        total_value += item.get('valor', 0)
                        
                        valor_formatado = f"R$ {item.get('valor', 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                        data_formatada = datetime.strptime(item['data_transferencia'], '%Y-%m-%d').strftime('%d/%m/%Y')

                        col1, col2 = st.columns([4, 1])
                        
                        with col1:
                            st.text(f"Recebedor: {item.get('nome_recebedor', 'N/A')}")
                            st.text(f"CNPJ/CPF: {item.get('cnpj_recebedor', 'N/A')}")
                            st.text(f"Valor: {valor_formatado}")
                            st.text(f"Data: {data_formatada}")

                        with col2:
                            pdf_url = item.get('pdf_url')
                            if pdf_url:
                                st.markdown(f'''<a href="{pdf_url}" target="_blank" style="display: inline-block; padding: 8px 16px; background-color: #FF4B4B; color: white; text-align: center; text-decoration: none; border-radius: 4px;">Baixar PDF</a>''', unsafe_allow_html=True)
                            else:
                                st.warning("PDF não disponível")
                        
                        if i < len(all_receipts_for_cnpj) - 1:
                            st.divider()

                    st.metric("💰 Total para este recebedor", f"R$ {total_value:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
