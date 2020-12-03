
class Utes:

    def wateraccount_to_catchmentNumber(self, WaterAccount):

        wateraccounts={
            'Bega and Brogo':'15481',
            'Belubula':'16801',
            'Border Rivers':'14681',
            'Cudgegong':'11984',
            'Gwydir':'11985',
            'Hunter':'12801',
            'Lachlan':'11983',
            'Lower Darling':'12104',
            'Lower Namoi':'11986',
            'Namoi':'11986',
            'Macquarie':'11984',
            'Murray':'11904',
            'Murrumbidgee':'11982',
            'Paterson':'13802',
            'Peel':'15101',
            'Richmond Regulated':'15324',
            'Upper Namoi':'12105'
        }
        #get the account code or return '' (default part of get() ) if not there
        wa = wateraccounts.get(WaterAccount, '')
        return( wa)



