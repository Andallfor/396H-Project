#!/usr/bin/env python3

# Basic code and comment structure copied from:
# https://huggingface.co/mmochtak/authdetect/blob/main/tutorial/how_to_use_authdetect_w_stanza.ipynb
# Modified for Reddit comments and posts as input

# Currently expects a .csv file with a doc_id (index) column and some column for the text (message by
# default, can be changed using --text-col when passing in arguments). All other columns will appear
# in the final .csv as they appear in the initial .csv

import simpletransformers.classification as cl
import stanza
import pandas as pd

import argparse

# Command line args
ap = argparse.ArgumentParser()
ap.add_argument("--input", default="sample_data.csv", help="input CSV")
ap.add_argument("--text-col", default="message", help="name of the column with input text")
ap.add_argument("--out-trigram", default="auth_per_trigram.csv", help="per-trigram output csv")
ap.add_argument("--out-text", default="auth_per_text.csv", help="per-text output csv")
args = ap.parse_args()

####################################################################
######################## 1) Processing CSV #########################
####################################################################

# Load data
df = pd.read_csv(args.input)

# Stanza pipeline for sentence splitting
p = stanza.Pipeline(lang="en", processors="tokenize")

# Split texts into sentences
sentences = []
for n in range(len(df[args.text_col])):
    doc = p(df[args.text_col][n])
    one_text = [sentence.text for sentence in doc.sentences]
    one_text_df = pd.DataFrame({
        "doc_id": n+1,
        "id": range(1, len(one_text) + 1),
        "text": one_text})
    sentences.append((one_text_df))

# Concatenate the list and reset the index.
sentences_final = pd.concat(sentences)
sentences_final.reset_index(drop=True, inplace=True)

####################################################################
##################### 2) Converting to trigram #####################
####################################################################

# Helper function for creating sentence trigrams. Sentence trigram is the main input for the authdetect model.
def create_ngram(text):
    no_steps = len(text) - 2
    indexes = [list(range(x, x + 3)) for x in range(no_steps)]
    ngrams = [' '.join(text[i] for i in index) for index in indexes]
    return ngrams

# Wrangle extracted sentences into sentence trigrams
all_data = []

u_docs = sentences_final["doc_id"].unique()

for n in range(len(u_docs)):
    one_doc = sentences_final[sentences_final['doc_id'] == u_docs[n]]
    text = one_doc['text'].tolist()

    if len(text) < 3:
      sentence_trigram = [' '.join(text)]
    else:
      # create trigrams
      sentence_trigram = create_ngram(text)

    # extract the first row excluding the 3th column
    doc_id = one_doc['doc_id'].iloc[0]

    # create a DataFrame with sentence trigrams
    sentence_df = pd.DataFrame({'sent_trigram': sentence_trigram})

    # add an 'id' and 'doc_id' column as standard refeferences
    sentence_df['id'] = range(1, len(sentence_trigram) + 1)
    sentence_df['doc_id'] = int(doc_id)

    all_data.append(sentence_df)

# concatenate all the new data into a final DataFrame
final = pd.concat(all_data, ignore_index=True)

####################################################################
####### 3) Applying authdetect model and outputting results ########
####################################################################

# Load authdetect model and predict (don't use multiprocessing for larger text since it causes stalling)
model = cl.ClassificationModel("roberta",
                               "mmochtak/authdetect", 
                               # args={"use_multiprocessing_for_evaluation": False,}
                               )

# Annotate the prepared trigrams with the authdetect model.
prediction = model.predict(to_predict = final["sent_trigram"].tolist())

# The `anno_df` data frame now contains an additional column
# called "predict" with the predictions made by the model. Since the classification model
# predicts the label (score) on a continuous scale, similar to a regression model,
# it can produce scores above or below the scale used for training (polyarchy index;
# 0-1 where 0 means almost exclusively associated with authoritarian discourse and
# 1 means almost exclusively associated with democratic discourse). Higher means more democratic,
# lower means more authoritarian discourse.
# It is worth mentioning that annotating 193 speeches containing 19,852 sentence trigrams
# took approximately 50 seconds (on a T4 GPU).
anno_df = final.assign(predict = prediction[1])

# Save the annotated data as a .csv file. The new file will be located in the same
# directory as the input file you uploaded at the beginning. If it does not
# appear automatically, click the refresh button (the circled arrow) to reload
# the folder's content. To download the file, right-click on it and select "Download"
# to save it to your local machine.
anno_df.to_csv(args.out_trigram, index=False)

# Notebook uses prediction[1] as the continuous score; mirror that
scores = prediction[1] if isinstance(prediction, tuple) and len(prediction) >= 2 else prediction

# In order to aggregate the scores of individual sentence trigrams back to the speech level,
# various strategies can be applied. The two most intuitive are the simple mean and the ratio of
# authoritarian trigrams.
# 'auth' is calculated as one minus the simple average of all trigrams per document.
# This gives a more intuitive 0-1 scale, where a higher score is more associated with authoritarian discourse.
#'auth_sent' captures the ratio of sentences that are associated with authoritarian discourse more than
# with democratic discourse (i.e., the assigned polyarchy index is lower than 0.5).
anno_df_speech = anno_df.groupby('doc_id').agg(
    demo=('predict', lambda x: x.mean()),
    auth=('predict', lambda x: 1 - x.mean()),
    auth_sent=('predict', lambda x: (x <= 0.5).mean()),
    num_sent=('predict', lambda x: x.size),
    predict_std=('predict', lambda x: x.std(ddof=0)),
).reset_index()

# Finally, concatenate the speech level scores with the original dataset (the one with the raw text)
# and save it as a .csv file.
df_final = pd.concat([df, anno_df_speech[['demo', 'auth', 'auth_sent', 'num_sent', 'predict_std']]], axis=1)
df_final.to_csv(args.out_text, index=False)