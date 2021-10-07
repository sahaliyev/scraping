from availables_engines import available_engines


def normalize_engine(engine):
    if len(str(engine)) == 2 and float(engine) > 16.0:
        value = int(engine) / 10
        if str(value) in available_engines:
            return value
    elif len(str(engine)) == 3 and '.' not in str(engine):
        value = int(engine) / 100
        if str(value) in available_engines:
            return value
    elif len(str(engine)) == 3 and '.' in str(engine) and str(engine) in available_engines:
        return float(engine)
    elif len(str(engine)) == 4:
        value = int(engine) / 1000
        if str(value) in available_engines:
            return value
    elif len(str(engine)) == 5:
        value = int(engine) / 1000
        if str(value) in available_engines:
            return value
    else:
        return


ban_types_dict = {
    # for tap.az
    'hetçbek': 'Hetçbek / Liftbek',
    'krosover': 'Offroader / SUV',
    'miniven': 'Minivan',
    'kabriolet': 'Kabrio',
    'offroader/suv': 'Offroader / SUV',
    #  for rover.az
    'krossover': 'Offroader / SUV',
    'offroader': 'Offroader / SUV',
    'suv': 'Offroader / SUV',
}

currency_dict = {
    'USD': '$',
    'AZN': 'AZN'
}

month_dict_for_tapaz = {'yanvar': '01', 'fevral': '02', 'mart': '03',
                        'aprel': '04', 'may': '05', 'iyun': '06',
                        'iyul': '07', 'avqust': '08', 'sentyabr': '09',
                        'oktyabr': '10', 'noyabr': '11', 'dekabr': '12'
                        }

marka_dict = {
    'qaz': 'gaz',
    'lada': 'lada (vaz)'
}
model_dict = {
    'cee`d': 'ceed',
    'star 2': 'star',
    'star 4500': 'star',
    'star truck': 'star',
    'xanter': 'hunter',
    'q 50 s': 'q50s',
    'ssangyong': 'Ssang Yong',
    'wingle-5': 'wingle',
    'a-15 cowin-amulet': 'a-15 cowin/amulet',
    'a113-a115': 'a113/a115',
    'a115': 'a113/a115',
    'a-15 cowin': 'a-15 cowin/amulet',
    'emgrand 7': 'emgrand 7 (rv)',
    'deer g3': 'deer',
    'florid cross': 'florid',
    'voleex c-30': 'voleex c30',
    'guattroporte': 'quattroporte',
    'tribeca b9': 'b9 tribeca'
}

details_dict = {
    'arxa kamera': 'arxa görüntü kamerası',
    'oturacaq ventilyasiyası': 'oturacaqların ventilyasiyası',
    'ön park radarı': 'park radarı',
    'arxa park radarı': 'park radarı'
}
