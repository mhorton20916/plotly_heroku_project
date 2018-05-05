from flask import Flask, jsonify, render_template
import pandas as pd

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

app = Flask(__name__)

engine = create_engine('sqlite:///db/belly_button_biodiversity.sqlite')
Base = automap_base()
Base.prepare(engine, reflect=True)

Samples = Base.classes.samples
Otu = Base.classes.otu
Samples_meta = Base.classes.samples_metadata

session = Session(engine)

@app.route('/')
def home():
    return(render_template('index.html'))

@app.route('/names')
def names():
    """List of sample names.

    Returns a list of sample names in the format
    [
        "BB_940",
        "BB_941",
        "BB_943",
        "BB_944",
        "BB_945",
        "BB_946",
        "BB_947",
        ...
    ]

    """
    names = Samples.__table__.columns.keys()[1:]
    return jsonify(names)
    # return names

@app.route('/otu')
def otu():
    """List of OTU descriptions.

    Returns a list of OTU descriptions in the following format

    [
        "Archaea;Euryarchaeota;Halobacteria;Halobacteriales;Halobacteriaceae;Halococcus",
        "Archaea;Euryarchaeota;Halobacteria;Halobacteriales;Halobacteriaceae;Halococcus",
        "Bacteria",
        "Bacteria",
        "Bacteria",
        ...
    ]
    """
    results = session.query(Otu.lowest_taxonomic_unit_found).all()
    # df = pd.DataFrame(results,columns=['otus'])
    otus, = zip(*results)
    otus_list = list(otus)
    return jsonify(otus_list)
    # return otus_list

@app.route('/metadata/<sample>')
def metadata(sample):
    """MetaData for a given sample.

    Args: Sample in the format: `BB_940`

    Returns a json dictionary of sample metadata in the format

    {
        AGE: 24,
        BBTYPE: "I",
        ETHNICITY: "Caucasian",
        GENDER: "F",
        LOCATION: "Beaufort/NC",
        SAMPLEID: 940
    }
    """
    inputID = int(sample[3:])
    results = session.query(Samples_meta.AGE,Samples_meta.BBTYPE,Samples_meta.ETHNICITY,\
        Samples_meta.GENDER,Samples_meta.LOCATION,Samples_meta.SAMPLEID).filter_by(SAMPLEID=inputID).one()
    df = pd.DataFrame([results],columns=['AGE','BBTYPE','ETHNICITY','GENDER','LOCATION','SAMPLEID'])
    output_dict = df.to_dict(orient='records')[0]
    return jsonify(output_dict)

@app.route('/wfreq/<sample>')
def wfreq(sample):
    """Weekly Washing Frequency as a number.

    Args: Sample in the format: `BB_940`

    Returns an integer value for the weekly washing frequency `WFREQ`
    """
    inputID = int(sample[3:])
    results = session.query(Samples_meta.WFREQ).filter_by(SAMPLEID=inputID).one()
    return jsonify(results[0])

@app.route('/samples/<sample>')
def samples(sample):
    """OTU IDs and Sample Values for a given sample.

    Sort your Pandas DataFrame (OTU ID and Sample Value)
    in Descending Order by Sample Value

    Return a list of dictionaries containing sorted lists  for `otu_ids`
    and `sample_values`

    [
        {
            otu_ids: [
                1166,
                2858,
                481,
                ...
            ],
            sample_values: [
                163,
                126,
                113,
                ...
            ]
        }
    ]
    """
    results = session.query(Samples)
    df = pd.read_sql(results.statement, results.session.bind)
    sample_df = pd.DataFrame(df[['otu_id',sample]][df[sample]>0])
    sample_df.sort_values(by=sample,ascending=False,inplace=True)
    otu_ids = [int(s) for s in sample_df['otu_id']]
    sample_values = [int(s) for s in sample_df[sample]]
    output_dict = {'otu_ids':otu_ids,'sample_values':sample_values}
    return jsonify(output_dict)

    
if __name__=='__main__':
    app.run(debug=True)