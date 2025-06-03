USE DATABASE STG;

USE SCHEMA public;

CREATE OR REPLACE TABLE CHAMBRE (
    NO_CHAMBRE INTEGER NOT NULL COMMENT 'Numéro de chambre',
    NOM_CHAMBRE VARCHAR(20) NOT NULL COMMENT 'Nom de la chambre',
    NO_ETAGE BYTEINT COMMENT 'Numéro d''étage',
    NOM_BATIMENT VARCHAR(20) COMMENT 'Nom du bâtiment',
    TYPE_CHAMBRE VARCHAR(10) COMMENT 'Type de chambre(simple, double)',
    PRIX_JOUR SMALLINT NOT NULL COMMENT 'Tarif journalier de la chambre',
    DT_CREATION DATE NOT NULL COMMENT 'Date de création de la chambre'
);

CREATE OR REPLACE TABLE TRAITEMENT (
    ID_TRAITEMENT INTEGER NOT NULL COMMENT 'Identifiant du traitement',
    CD_MEDICAMENT INTEGER NOT NULL COMMENT 'Code du médicament',
    CATG_MEDICAMENT VARCHAR(100) NOT NULL COMMENT 'Catégorie de médicament',
    MARQUE_FABRI VARCHAR(100) NOT NULL COMMENT 'Marque du fabricant du médicament',
    QTE_MEDICAMENT SMALLINT COMMENT 'Quantité du médicament',
    DSC_POSOLOGIE VARCHAR(100) NOT NULL COMMENT 'Description de la posologie',
    ID_CONSULT INTEGER NOT NULL COMMENT 'Identifiant de la consultation',
    TS_CREATION_TRAITEMENT TIMESTAMP(0) NOT NULL COMMENT 'Date heure de création du traitement'
);

CREATE OR REPLACE TABLE PERSONNEL (
    ID_PERSONNEL INTEGER NOT NULL COMMENT 'Identifiant du personnel',
    NOM_PERSONNEL VARCHAR(100) NOT NULL COMMENT 'Nom du personnel',
    PRENOM_PERSONNEL VARCHAR(100) NOT NULL COMMENT 'Prénom du personnel',
    FONCTION_PERSONNEL VARCHAR(50) NOT NULL COMMENT 'Fonction du personnel',
    TS_DEBUT_ACTIVITE TIMESTAMP(0) NOT NULL COMMENT 'Date heure de début d''activité à l''hôpital',
    TS_FIN_ACTIVITE TIMESTAMP(0) COMMENT 'Date heure de fin d''activité à l''hôpital',
    RAISON_FIN_ACTIVITE VARCHAR(100) COMMENT 'Raison de la fin d''activité',
    TS_CREATION_PERSONNEL TIMESTAMP(0) NOT NULL COMMENT 'Date heure de création du personnel',
    TS_MAJ_PERSONNEL TIMESTAMP(0) NOT NULL COMMENT 'Date heure de mise à jour du personnel',
    CD_STATUT_PERSONNEL VARCHAR(10) NOT NULL COMMENT 'Code statut du personnel (actif, inactif, suspendu, résilié)'
);

CREATE OR REPLACE TABLE PATIENT (
    ID_PATIENT INTEGER NOT NULL COMMENT 'Identifiant du patient',
    NOM_PATIENT VARCHAR(100) NOT NULL COMMENT 'Nom du patient',
    PRENOM_PATIENT VARCHAR(100) NOT NULL COMMENT 'Prénom du patient',
    DT_NAISS DATE COMMENT 'Date de naissance du patient',
    VILLE_NAISS VARCHAR(100) COMMENT 'Ville de naissance du patient',
    PAYS_NAISS VARCHAR(100) COMMENT 'Pays de naissance du patient',
    NUM_SECU VARCHAR(15) COMMENT 'Numéro de sécurité sociale',
    IND_PAYS_NUM_TELP VARCHAR(5) COMMENT 'Indicatif du pays',
    NUM_TELEPHONE VARCHAR(20) COMMENT 'Numéro de téléphone du patient',
    NUM_VOIE VARCHAR(10) COMMENT 'Numéro de voie de l''adresse postale',
    DSC_VOIE VARCHAR(250) COMMENT 'Libellé de voie de l''adresse postale',
    CMPL_VOIE VARCHAR(250) COMMENT 'Complément d''adresse',
    CD_POSTAL VARCHAR(10) COMMENT 'Code postal',
    VILLE VARCHAR(100) COMMENT 'Ville',
    PAYS VARCHAR(100) COMMENT 'Pays',
    TS_CREATION_PATIENT TIMESTAMP(0) NOT NULL COMMENT 'Date heure de création du patient',
    TS_MAJ_PATIENT TIMESTAMP(0) NOT NULL COMMENT 'Date heure de mise à jour du patient'
);

CREATE OR REPLACE TABLE CONSULTATION (
    ID_CONSULT INTEGER NOT NULL COMMENT 'Identifiant de la consultation',
    ID_PERSONNEL INTEGER NOT NULL COMMENT 'Identifiant du personnel',
    ID_PATIENT INTEGER NOT NULL COMMENT 'Identifiant du patient',
    TS_DEBUT_CONSULT TIMESTAMP(0) NOT NULL COMMENT 'Date heure de début de la consultation',
    TS_FIN_CONSULT TIMESTAMP(0) NOT NULL COMMENT 'Date heure de fin de la consultation',
    POIDS_PATIENT INTEGER NOT NULL COMMENT 'Poids du patient',
    TEMP_PATIENT INTEGER COMMENT 'Température du patient',
    UNIT_TEMP VARCHAR(15) COMMENT 'Unité de température',
    TENSION_PATIENT INTEGER COMMENT 'Tension du patient',
    DSC_PATHO VARCHAR(250) COMMENT 'Description de la pathologie',
    INDIC_DIABETE VARCHAR(10) COMMENT 'Positif au diabète ? True/False',
    ID_TRAITEMENT INTEGER COMMENT 'Identifiant du traitement',
    INDIC_HOSPI VARCHAR(10) COMMENT 'Consultation entraînant une hospitalisation ? True/False'
);

CREATE OR REPLACE TABLE HOSPITALISATION (
    ID_HOSPI INTEGER NOT NULL COMMENT 'Identifiant de l''hospitalisation',
    ID_CONSULT INTEGER NOT NULL COMMENT 'Identifiant de la consultation',
    NO_CHAMBRE SMALLINT NOT NULL COMMENT 'Numéro de chambre',
    TS_DEBUT_HOSPI TIMESTAMP(0) NOT NULL COMMENT 'Date heure de début de l''hospitalisation',
    TS_FIN_HOSPI TIMESTAMP(0) COMMENT 'Date heure de fin de l''hospitalisation',
    COUT_HOSPI TIMESTAMP(0) COMMENT 'Coût de l''hospitalisation',
    ID_PERSONNEL_RESP INTEGER NOT NULL COMMENT 'Identifiant du personnel en charge de l''hospitalisation'
);

CREATE OR REPLACE TABLE MEDICAMENT (
    CD_MEDICAMENT VARCHAR(10) NOT NULL COMMENT 'Code du médicament',
    NOM_MEDICAMENT VARCHAR(250) COMMENT 'Nom du médicament',
    CONDIT_MEDICAMENT VARCHAR(100) COMMENT 'Conditionnement du médicament',
    CATG_MEDICAMENT VARCHAR(100) NOT NULL COMMENT 'Catégorie de médicament',
    MARQUE_FABRI VARCHAR(100) NOT NULL COMMENT 'Marque du fabricant du médicament'
);
