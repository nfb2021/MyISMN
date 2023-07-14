# import flaghandler
from flaghandler import Flags
from datahandler import DataReader


class GroupDynamicVariable(Flags):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "dynamic variables"


class GroupC(Flags):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "reported value exceeds output format field size"


class GroupD(Flags):
    category = "D"

    def __init__(self):
        super().__init__()
        # self.category = 'D'

    def __str__(self):
        return "questionable/dubious"


class GeophyscialBased(GroupD):
    sub_category = "geophysical based"

    def __init__(self):
        super().__init__()
        # self.sub_category = 'geophysical based'

    def __str__(self):
        return "geophysical based"


class SpectrumBased(GroupD):
    # def __init__(self):
    #     super().__init__()

    def __str__(self):
        return "spectrum based"


class G(GroupDynamicVariable):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "Good"


class M(GroupDynamicVariable):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "Parameter value missing"


class C01(GroupC):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "soil moisture < 0.0 m^3/m^3"


class C02(GroupC):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "soil moisture > 0.6 m^3/m^3"


class C03(GroupC):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "soil moisture > saturation point (derived from HWSD parameter values)"


class D01(GeophyscialBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "in situ soil temperature (at corresponding depth layer) < 0°C"


class D02(GeophyscialBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "in situ air temperature < 0°C"


class D03(GeophyscialBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "GLDAS soil temperatureat corresponding depth layer) < 0°C"


class D04(GeophyscialBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "soil moisture shows peaks without precipitation event (in situ) \
            in the preceding 24 hours"


class D05(GeophyscialBased):
    kind = "D05"

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "soil moisture shows peaks without precipitation event (GLDAS) \
            in the preceding 24 hours"


class D06(SpectrumBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "a spike is detected in soil moisture spectrum"


class D07(SpectrumBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "a negative jump is detected in soil moisture spectrum"


class D08(SpectrumBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "a positive jump is detected in soil moisture spectrum"


class D09(SpectrumBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "low constant values (for a minimum time of 12 hours) occur in \
            soil moisture spectrum"


class D10(SpectrumBased):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return "saturated plateau (for a minimum time length of 12 hours) occurs in \
            soil moisture spectrum"


class FunWithFlags(Flags, DataReader):
    """Small class centered around flags of the ISMN databse.
    Concerning soil moisture, following flags exist:

    Dynamic variables
    -----------------
    G:   Good
    M:   Parameter value missing

    C - reported value exceeds output format field size
    ---------------------------------------------------
    C01: soil moisture < 0.0 m^3/m^3
    C02: soil moisture > 0.6 m^3/m^3
    C03: soil moisture > saturation point (derived from HWSD parameter values)

    D - questionable/dubious - geophysical based
    --------------------------------------------
    D01: in situ soil temperature(*) < 0°C
    D02: in situ air temperature < 0°C
    D03: GLDAS soil temperature(*) < 0°C
    D04: soil moisture shows peaks without precipitation event (in situ) \
        in the preceding 24 hours
    D05: soil moisture shows peaks without precipitation event (GLDAS) \
        in the preceding 24 hours

    D - questionable/dubious - spectrum based
    -----------------------------------------
    D06: a spike is detected in soil moisture spectrum
    D07: a negative jump is detected in soil moisture spectrum
    D08: a positive jump is detected in soil moisture spectrum
    D09: low constant values (for a minimum time of 12 hours) occur in \
        soil moisture spectrum
    D10: saturated plateau (for a minimum time length of 12 hours) occurs \
        in soil moisture spectrum

    (*) at corresponding depth layer
    """

    def __init__(self):
        super().__init__()

    # def __repr__(self):
    #     print(f'Small class centered around flags of the ISMN databse.
    #     Conserning soil moisture, there could be following flags:\n \

    #     Dynamic variables
    #     -----------------
    #     G\tGood
    #     M\tParameter value missing

    #     C - reported value exceeds output format field size
    #     ---------------------------------------------------
    #     C01\tsoil moisture < 0.0 m^3/m^3
    #     C02\tsoil moisture > 0.6 m^3/m^3
    #     C03\tsoil moisture > saturation point (derived from HWSD parameter values)

    #     D - questionable/dubious - geophysical based
    #     --------------------------------------------
    #     D01\tin situ soil temperature(*) < 0°C
    #     D02\tin situ air temperature < 0°C
    #     D03\tGLDAS soil temperature(*) < 0°C
    #     D04\tsoil moisture shows peaks without precipitation event (in situ) \
    # in the preceding 24 hours
    #     D05\tsoil moisture shows peaks without precipitation event (GLDAS) \
    # in the preceding 24 hours

    #     D - questionable/dubious - spectrum based
    #     -----------------------------------------
    #     D06\ta spike is detected in soil moisture spectrum
    #     D07\ta negative jump is detected in soil moisture spectrum
    #     D08\ta positive jump is detected in soil moisture spectrum
    #     D09\tlow constant values (for a minimum time of 12 hours) occur \
    # in soil moisture spectrum
    #     D10\tsaturated plateau (for a minimum time length of 12 hours) occurs \
    # in soil moisture spectrum

    #     (*) at corresponding depth layer
    #     ')
