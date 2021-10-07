import traceback
from datetime import datetime

from db_operations import DbModel


class DBQueries:
    def __init__(self):
        self.db = DbModel()

    def get_reqress(self, latest_id):
        sql = f"select ASM_SENED_ID as ASM_SƏNƏD_ID, ASM_SENED_TARIX, ASM_SENED_NOMRESI, ASM_NISYE_MEBLEG, ASM_MUDDET, ASM_MUSHTERI_TAM_ADI, TG_SAZISH_SENED_NOMRESI, " \
              f"ASM_SENED_ID, HESAB_23240_QALIQ, HESAB_23243_QALIQ, HESAB_23302_QALIQ, HESAB_23303_QALIQ, TOPLAM_QALIQ_BORC, FAKTORINQ_KOM1_FAIZI," \
              f"FAKTORINQ_KOM4_FAIZI, GUN_SAYI, DUZELISH_GUN_SAYI, AY, GUZESHT_KOM1_FAIZI, GUZESHT_KOM4_FAIZI, IFO_MEBLEGI, QFO_MEBLEGI, IFO_REQRES, " \
              f"QFO_REQRES, KOM1_TUTULMA_IFO, KOM4_TUTULMA, ASM_MUSHTERI_ID " \
              f"from fr360.TB_TG_REQRES_UZRE_ASM@DBLNK_FROLTP_STB " \
              f"where ASM_SENED_ID > :ASM_SENED_ID " \
              f"order by ASM_SENED_ID"
        val = dict(ASM_SENED_ID=latest_id)
        # val = None
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def get_hesablar(self, latest_id):
        sql = """select hesab.HESAB_ID as "Kod", 
                hesab.HESAB_NOMRESI as "Hesab nömrəsi",
                hesab.HESAB_SAHIBI_TAM_ADI as "Müştəri",
                filial.SHIRKET_FILIAL_adi as "Filial",
                valyuta.valyuta_qisa_adi as "Valyuta",
                hesab.EN_SON_BALANS_MEBLEG as "Qalıq" , 
                hesab.EN_SON_BALANS_MEBLEG_MV as "Qalıq1",  
                hesab.HESAB_REF as "HesabRef"
                from fr360.V_ACC_HESAB1@DBLNK_FROLTP_STB hesab
                left join fr360.ACC_VALYUTA@DBLNK_FROLTP_STB valyuta on valyuta.valyuta_id=hesab.VALYUTA_ID
                left join fr360.cmn_shirket_filial@DBLNK_FROLTP_STB filial on filial.SHIRKET_FILIAL_ID=hesab.SHIRKET_FILIAL_ID
                where hesab.HESAB_ID > :HESAB_ID
                order by hesab.HESAB_ID"""
        val = dict(HESAB_ID=latest_id)
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def get_provodkalar(self, latest_id):
        sql = """select SENED_ID as "Sənəd_ID",
                    SENED_NOMRESI as "Sənəd nömrəsi", 
                    SENED_TARIXI as "Tarix",
                    HESAB_REF as "Hesab Ref",
                    MEBLEG as "Məbləğ",
                    DEBIT_HESAB_NOMRESI as "Debit hesab",
                    KREDIT_HESAB_NOMRESI as "Kredit hesab",
                    EMELIYYAT_MENBE_ADI as "Mənbə",
                    EMELIYYAT_TIP_ADI as "Tip",
                    EMELIYYAT_TEYINAT_ADI as "Təyinat",
                    SENED_STATUS_ADI as "Sənəd Statusu"
                    from fr360.V_DOC_ACC_EMELIYYAT1@DBLNK_FROLTP_STB
                    where SENED_ID > :SENED_ID
                    order by SENED_ID"""
        val = dict(SENED_ID=latest_id)
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def get_customers(self, latest_id):
        sql = """select MUSHTERI_FIZIKI_SHEXS_ID as "ID",
                    ADI , 
                    SOYADI, 
                    ATA_ADI, 
                    FINKOD, 
                    SH_VESIQE_SERIYASI as "Şəxsiyyət vəsiqəsinin seriyası", 
                    SH_VESIQE_NOMRESI as "Şəxsiyyət vəsiqəsinin nömrəsi" 
                    from fr360.CT_MUSHTERI_FIZIKI_SHEXS@DBLNK_FROLTP_STB
                    where MUSHTERI_FIZIKI_SHEXS_ID > :MUSHTERI_FIZIKI_SHEXS_ID
                    order by MUSHTERI_FIZIKI_SHEXS_ID"""
        val = dict(MUSHTERI_FIZIKI_SHEXS_ID=latest_id)
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def get_terminal(self, latest_id):
        sql = """select *
                    from fr360.CT_TRM_PAYMENTS@DBLNK_FROLTP_STB 
                    where PAYMENT_ID > :PAYMENT_ID
                    order by PAYMENT_ID"""
        val = dict(PAYMENT_ID=latest_id)
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def finance_monthly_report(self, latest_id):
        sql = """select * from fr360.DOC_TG_SAZISH@DBLNK_FROLTP_STB 
                    where SENED_ID > :SENED_ID
                    order by SENED_ID"""
        val = dict(SENED_ID=latest_id)
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def sazish(self, latest_id):
        sql = """select ASM_SENED_ID, ASM_SENED_TARIX, 
                ASM_SENED_NOMRESI, 
                ASM_MEBLEG, 
                ASM_ILKIN_ODENISH, 
                ASM_NISYE_MEBLEG, 
                ASM_MUDDET, 
                ASM_MUSHTERI_TAM_ADI, 
                ASM_MEHSUL_ADI, 
                ASM_MEHSUL_RISK_FAIZI, 
                FAKTORINQ_KOM1_FAIZI, 
                FAKTORINQ_KOM1_MEBLEGI, 
                KOM4_FAIZI, 
                KOM4_MEBLEGI, 
                IFO_FAIZI, 
                IFO_MEBLEGI, 
                QFO_MEBLEGI, 
                ASM_MUSHTERI_ID from fr360.TB_TG_SAZISH_UZRE_ASM@DBLNK_FROLTP_STB
                where ASM_SENED_ID > :ASM_SENED_ID
                order by ASM_SENED_ID"""
        val = dict(ASM_SENED_ID=latest_id)
        return self.db.get_many_items_from_db_as_dict(sql, val)
