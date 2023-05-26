class Flag:
    '''Defines flags used by the ISMN data quality control'''

    def __str__(self):
        return 'A class representing a flag, as used as quality inicator for ISMN data'


class SoilMoisture(Flag):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'ISMN Flags used with soil moisture data'


class BaseCategoryDynamicVariable(SoilMoisture):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'The "Dynamic Variable" category is one of the three base categories of ISMN flags: variables that are dynamic'


class BaseCategoryC(SoilMoisture):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'The "C" category is one of the three base categories of ISMN flags: variables that reported value exceeds output format field size'


class BaseCategoryD(SoilMoisture):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'The "D" category is one of the three base categories of ISMN flags: variables that are questionable/dubious'


class SubCategoryGeophyscialBased(BaseCategoryD):
    # sub_category = 'geophysical based'

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'A sub-category of ISMN flag category "D": quality control parameters that are geophysical based'


class SubCategorySpectrumBased(BaseCategoryD):
    # sub_category = 'spectrum based'

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'A sub-category of ISMN flag category "D": quality control parameters that are spectrum based'


class G(BaseCategoryDynamicVariable):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Flag meaning: Good'


class M(BaseCategoryDynamicVariable):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Flag meaning: Parameter value missing'


class C01(BaseCategoryC):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Flag meaning: Flag meaning: soil moisture < 0.0 m^3/m^3'


class C02(BaseCategoryC):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Flag meaning: soil moisture > 0.6 m^3/m^3'


class C03(BaseCategoryC):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Flag meaning: soil moisture > saturation point (derived from HWSD parameter values)'


class D01(SubCategoryGeophyscialBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Flag meaning: in situ soil temperature (at corresponding depth layer) < 0°C'


class D02(SubCategoryGeophyscialBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Flag meaning: in situ air temperature < 0°C'


class D03(SubCategoryGeophyscialBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Flag meaning: GLDAS soil temperatureat corresponding depth layer) < 0°C'


class D04(SubCategoryGeophyscialBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Flag meaning: soil moisture shows peaks without precipitation event (in situ) in the preceding 24 hours'


class D05(SubCategoryGeophyscialBased):
    kind = 'D05'

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Flag meaning: soil moisture shows peaks without precipitation event (GLDAS) in the preceding 24 hours'


class D06(SubCategorySpectrumBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Flag meaning: a spike is detected in soil moisture spectrum'


class D07(SubCategorySpectrumBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Flag meaning: a negative jump is detected in soil moisture spectrum'


class D08(SubCategorySpectrumBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Flag meaning: a positive jump is detected in soil moisture spectrum'


class D09(SubCategorySpectrumBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Flag meaning: low constant values (for a minimum time of 12 hours) occur in soil moisture spectrum'


class D10(SubCategorySpectrumBased):
    '''Flag meaning: saturated plateau (for a minimum time length of 12 hours) occurs in soil moisture spectrum'''

    # kind = 'D10'

    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Flag meaning: saturated plateau (for a minimum time length of 12 hours) occurs in soil moisture spectrum'

    def __repr__(self):
        return 'Flag meaning: saturated plateau (for a minimum time length of 12 hours) occurs in soil moisture spectrum'


test_flag = D10
print(test_flag)