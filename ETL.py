# imports
import pandas as pd


if __name__ == '__main__':
    
    # read in files
    df_1 = pd.read_csv('./data/cons.csv')
    df_2 = pd.read_csv('./data/cons_email.csv')
    df_3 = pd.read_csv('./data/cons_email_chapter_subscription.csv')

    # merged the first two datasets while getting rid of any email that isn't the primary email
    df_12 = df_1.merge(df_2, how='left', on='cons_id')
    df_12_primary = df_12[df_12['is_primary'] == 1.0]

    # merged the merged dataset with the last dataset while getting rid of any constituent that doesn't have their chapter_id value equalling 1
    df_123 = df_12_primary.merge(df_3, how='left', on='cons_email_id')
    df_123_chapter_id = df_123[df_123['chapter_id'] == 1.0]

    # selected the columns that were a part of the schema asked for while renaming them to the appropriate column names and outputed the file
    df = df_123_chapter_id[['email','source','isunsub','create_dt_x','modified_dt_x']]
    df = df.rename(columns={"source": "code", "isunsub": "is_unsub", "create_dt_x": "created_dt", "modified_dt_x": "updated_dt"})
    df = df.reset_index().drop('index',axis=1)
    df.to_csv('./output/people.csv')

    # got the calendar dates from the person creation time, created the acquisition dataframe from the series obtained, and outputed the file
    df['created_calendar_dt'] = pd.to_datetime(df['created_dt']).dt.date
    df_aquisition = df.sort_values('created_calendar_dt').groupby('created_calendar_dt').size().reset_index()
    df_aquisition = df_aquisition.rename(columns={"created_calendar_dt": "acquisition_date", 0: "acquisitions"})
    df_aquisition.to_csv('./output/acquisition_facts.csv')
