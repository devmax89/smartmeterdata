# Utilizza l'immagine ufficiale di PostgreSQL
FROM postgres:latest

# Imposta le variabili d'ambiente per il nome del database e l'utente
ENV POSTGRES_DB=smartmetering
ENV POSTGRES_USER=admin
ENV POSTGRES_PASSWORD=admin

# Aggiunge lo script SQL al docker image
ADD create_table.sql /docker-entrypoint-initdb.d/