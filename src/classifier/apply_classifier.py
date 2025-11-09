#!/usr/bin/env python3

# Basic code and comment structure copied from:
# https://huggingface.co/mmochtak/authdetect/blob/main/tutorial/how_to_use_authdetect_w_stanza.ipynb
# Modified for Reddit comments and posts as input
#
# Expects a CSV with:
#   - an ID column (default: "id")
#   - a text column (default: "body")
# All other columns are copied over to the final CSV.

import argparse
from pathlib import Path

import pandas as pd
import stanza
import simpletransformers.classification as cl

# Command line args
ap = argparse.ArgumentParser()
ap.add_argument("--input", default="sample_data.csv", help="input CSV (filename only)")
ap.add_argument(
    "--text-col",
    default="body",
    help="name of the column with input text",
)
ap.add_argument(
    "--id-col",
    default="id",
    help="name of the column that uniquely identifies each text row",
)
args = ap.parse_args()

UNFINISHED_DIR = Path("./csvs/unfinished")
FINISHED_DIR = Path("./csvs/finished")
FINISHED_DIR.mkdir(parents=True, exist_ok=True)

input_path = UNFINISHED_DIR / args.input
stem = Path(args.input).stem
out_trigram_path = FINISHED_DIR / f"{stem}_per_trigram.csv"
out_text_path = FINISHED_DIR / f"{stem}_per_text.csv"

id_col = args.id_col  # reuse this name everywhere

####################################################################
######################## 1) Processing CSV #########################
####################################################################

# Load data from unfinished
df = pd.read_csv(input_path)

if args.text_col not in df.columns:
    raise ValueError(f"Text column '{args.text_col}' not found in input CSV.")
if id_col not in df.columns:
    raise ValueError(f"ID column '{id_col}' not found in input CSV.")

# Stanza pipeline for sentence splitting
p = stanza.Pipeline(lang="en", processors="tokenize")

# Split texts into sentences using list buffers
buf_docid = []   # original document ID from df[id_col]
buf_sent_id = [] # sentence index within document
buf_text = []    # sentence text

texts = df[args.text_col].fillna("").tolist()
doc_ids = df[id_col].tolist()

for doc_id, text in zip(doc_ids, texts):
    doc = p(text)
    for sent_id, sentence in enumerate(doc.sentences, start=1):
        buf_docid.append(doc_id)
        buf_sent_id.append(sent_id)
        buf_text.append(sentence.text)

sentences_final = pd.DataFrame(
    {
        id_col: buf_docid,   # same column name as in the original CSV
        "sent_id": buf_sent_id,
        "text": buf_text,
    }
)

####################################################################
##################### 2) Converting to trigram #####################
####################################################################

# Helper function for creating sentence trigrams.
# Sentence trigram is the main input for the authdetect model.
def create_ngram(text_list):
    no_steps = len(text_list) - 2
    indexes = [list(range(x, x + 3)) for x in range(no_steps)]
    ngrams = [" ".join(text_list[i] for i in idx) for idx in indexes]
    return ngrams


# Wrangle extracted sentences into sentence trigrams using buffers
u_docs = sentences_final[id_col].unique()

cp = sentences_final[id_col].to_numpy()
tt = sentences_final["text"].to_numpy()
jj = len(cp)

buf_tri = []
buf_tri_docid = []
buf_tri_id = []

count_docs = len(u_docs)
j = 0

for n in range(count_docs):
    doc_id = u_docs[n]

    # collect all sentences for this document
    text_list = []
    while j < jj and cp[j] == doc_id:
        text_list.append(tt[j])
        j += 1

    if len(text_list) < 3:
        sentence_trigram = [" ".join(text_list)]
    else:
        sentence_trigram = create_ngram(text_list)

    for trig_id, trig in enumerate(sentence_trigram, start=1):
        buf_tri.append(trig)
        buf_tri_id.append(trig_id)
        buf_tri_docid.append(doc_id)

final = pd.DataFrame(
    {
        id_col: buf_tri_docid,      # original ID column
        "trigram_id": buf_tri_id,   # index within document
        "sent_trigram": buf_tri,
    }
)

####################################################################
####### 3) Applying authdetect model and outputting results ########
####################################################################

# Load authdetect model and predict
model = cl.ClassificationModel(
    "roberta",
    "mmochtak/authdetect",
    args={"use_multiprocessing_for_evaluation": False},
    use_cuda=False,
)

# Annotate the prepared trigrams with the authdetect model.
prediction = model.predict(to_predict=final["sent_trigram"].tolist())

scores = prediction[1] if isinstance(prediction, tuple) and len(prediction) >= 2 else prediction

anno_df = final.assign(predict=scores)
anno_df.to_csv(out_trigram_path, index=False)  # write to finished

# Aggregate to per-text (per original id) scores
anno_df_speech = (
    anno_df.groupby(id_col)
    .agg(
        demo=("predict", lambda x: x.mean()),
        auth=("predict", lambda x: 1 - x.mean()),
        auth_sent=("predict", lambda x: (x <= 0.5).mean()),
        predict_std=("predict", lambda x: x.std(ddof=0)),
    )
    .reset_index()
)

# Merge aggregated scores back onto the original dataframe by its ID column
df_final = df.merge(
    anno_df_speech,
    on=id_col,
    how="left",
)

df_final.to_csv(out_text_path, index=False)  # write to finished
