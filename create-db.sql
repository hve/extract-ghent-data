BEGIN;

CREATE TABLE "datum" (
	"datum_id" INTEGER NOT NULL,
	"jaar" INTEGER NOT NULL,
	"maand" INTEGER NOT NULL,
	"dag" INTEGER NOT NULL,
	"kwartaal_nummer" INTEGER NOT NULL,
	"week_nummer" INTEGER NOT NULL,
	PRIMARY KEY("datum_id")
);

CREATE TABLE "stadswijk" (
    "stadswijk_id" INTEGER NOT NULL,
	"stadswijk_naam" TEXT,
	"stadsdeel" TEXT,
	"oppervlakte_km2" DOUBLE,
	"oppervlakte_m2" DOUBLE,
	"oppervlakte_ha" DOUBLE,
	PRIMARY KEY("stadswijk_id")
);

CREATE TABLE "stadswijk_bevolkingsaantal" (
	"jaar" INTEGER NOT NULL,
	"stadswijk_id" INTEGER NOT NULL,
	"bevolkingsaantal" BIGINT,
	PRIMARY KEY("jaar","stadswijk_id")
);

CREATE TABLE "bevolkingsaantal" (
	"jaar" INTEGER NOT NULL UNIQUE,
	"bevolkingsaantal" BIGINT,
	PRIMARY KEY("jaar")
);

CREATE TABLE "misdrijf" (
	"jaar_maand" BIGINT NOT NULL,
	"stadswijk_id" INTEGER NOT NULL,
	"misdrijf_categorie_id" INTEGER NOT NULL,
	"misdrijf_aantal" BIGINT,
	"misdrijf_kwartaal_totaal" BIGINT,
	"misdrijf_jaar_totaal" BIGINT,
	PRIMARY KEY("jaar_maand","stadswijk_id","misdrijf_categorie_id")
);

CREATE TABLE "misdrijf_categorie" (
	"misdrijf_categorie_id" INTEGER NOT NULL,
	"misdrijf_categorie_naam" TEXT,
	PRIMARY KEY("misdrijf_categorie_id")
);

COMMIT;