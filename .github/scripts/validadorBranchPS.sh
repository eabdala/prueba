#!/usr/bin/env bash

# shellcheck disable=SC2086

if [[ $GITHUB_EVENT_NAME != "push" && $GITHUB_EVENT_NAME != "pull_request" ]]; then
  echo "::warning title=unsupported::action ran on unsupported event ${GITHUB_EVENT_NAME}"
  exit 0
fi

if [[ -z $BASE_SHA && $GITHUB_EVENT_NAME == "push" ]]; then
  BASE_SHA="HEAD~$(jq '.commits | length' "${GITHUB_EVENT_PATH}")" # push events
fi

CHANGED="$(git diff --exit-code --quiet ${BASE_SHA} HEAD -- ${DIFF_PATHS} && echo 'false' || echo 'true')"
FILES="$(git diff --name-only ${BASE_SHA} HEAD -- ${DIFF_PATHS} | tr '\n' ' ')"
echo "$(git diff --name-only ${BASE_SHA} HEAD -- ${DIFF_PATHS})"
# echo $FILES | tr '\n' ' '
echo "changed=${CHANGED}" >> "${GITHUB_OUTPUT}"

if [[ $FILES ]]; then
  echo "files=${FILES}" >> "${GITHUB_OUTPUT}"
  echo "json=$(jq --compact-output --null-input '$ARGS.positional' --args -- "${FILES}")" >> "${GITHUB_OUTPUT}"
else
  echo "json=[]" >> "${GITHUB_OUTPUT}"
fi


# URL=$2
# repo_name=$(basename "$URL" .git)
# CURRENT_DIR=$(dirname ${BASH_SOURCE[0]})
# APP_PATH=${CURRENT_DIR}/..

# if [ -d "$repo_name" ]; then
#       printf "La carpeta del repo existe. \n"
#       cd ${repo_name}
# else
#       printf "La carpeta del repo no existe. \n"

#       printf "Clonar repo si no existe \n"
#       git clone -q ${URL}
#       cd ${repo_name}
# fi

python_inst=$(command -v python &> /dev/null;)
if [ -z "$python_inst" ]; then
    printf "Python está instalado. \n"
    nombre_modulo="autoflake"

      # Verificar si el módulo está instalado
      if python -c "import $nombre_modulo" &> /dev/null; then
         printf "$nombre_modulo ya está instalado. \n"
      else
         # Instalar el módulo si no está instalado
         printf "Instalando $nombre_modulo... \n"
         pip install $nombre_modulo
      fi
else
    printf "Python no está instalado. \n"
fi



# printf "Checkout a la rama y pull de novedades \n"
# git checkout -f -q $1
# git pull -q
# echo " - Done"

COUNTER_ERROR=0
FILE_COUNTER=0

# files=`git diff origin/master... --name-only --diff-filter=d`

printf "\n\e[0;36m ********* Archivos modificados ********* \e[m\n\n"

IFS_SAVE=$IFS  # Guardar el valor actual de IFS
IFS=' '        # Establecer IFS para que use el espacio como delimitador
for file in $FILES; do
   echo $file
done

printf "\n\e[0;36m ********* Comienzo de validaciones ********* \e[m\n\n"


for file in $FILES; do
   FILE_COUNTER_ERROR=0
   let FILE_COUNTER++

    printf 'Archivo: %s \n' $file

   if [[ $file == analysis* ]];
   then
      if [ -z "$python_inst" ]; then
         if [[ $file == *.py ]]; then
            cambios=$(python -m autoflake -c --remove-unused-variables --remove-unused-variables "$file")
            # Verificar si la salida contiene la cadena "Unused imports/variables detected"
            if [[ "$cambios" == *"Unused imports/variables detected"* ]]; then
               printf "\e[0;31m  - Se detectaron import/variables en el archivo sin usar. \e[m\n"
               # Realizar acciones adicionales si es necesario
               nombre_archivo=$(basename "$file")
               python -m autoflake -cd --remove-unused-variables --remove-unused-variables "$file" > "$APP_PATH/fixed_$nombre_archivo"
               let COUNTER_ERROR++,FILE_COUNTER_ERROR++
            # else
            #    printf "\e[0;31m  - No se detectaron cambios en el archivo. \e[m\n"
            fi
      
         fi
      fi


      if [[ "$file" =~ ^(notebooks[\/]Users) ]];
      then
         printf '\e[0;31m  - Databricks notebook ERROR: Las notebooks no deben estar en los directorios particulares de los usuarios. \e[m\n'
         let COUNTER_ERROR++,FILE_COUNTER_ERROR++
      fi

      # if ! [[ "$file" =~ ([\/]([A-Z][a-z]+)+([\_]([A-Z][a-z]+)+){3}.py)$ ]];
      # then
      #    printf '\e[0;31m  - Databricks notebook ERROR: Revisar la nomenclatura. \e[m\n'
      #    let COUNTER_ERROR++,FILE_COUNTER_ERROR++
      # fi
  	
      if grep -iq "truckprd\|lkprd\|consumezoneprod\|historyzoneprod\|prelandingzoneprod" $file 
      then
         printf "\e[0;31m  - Databricks notebook ERROR: Eliminar las referencias a tablas externas productivas truckprd o lkprd y path productivos consumezoneprod, historyzoneprod y prelandingzoneprod. \e[m\n"
         let COUNTER_ERROR++,FILE_COUNTER_ERROR++
      fi

      if grep -iq "print(\|display(" $file
      then
         printf "\e[0;31m  - Databricks notebook ERROR: Eliminar prints o displays (Los comentarios deben ir en celdas con lenguaje md). \e[m\n"
         let COUNTER_ERROR++,FILE_COUNTER_ERROR++
      fi

	   if grep -iq "dbutils.fs\|%fs\|%sh" $file
      then
         printf "\e[0;31m  - Databricks notebook ERROR: Eliminar todos los comandos sh o fs. \e[m\n"
         let COUNTER_ERROR++,FILE_COUNTER_ERROR++
      fi

      if [[ $file != *.py ]]; 
      then
         printf "\e[0;31m  - Databricks notebook ERROR: La extension de la notebook debe ser .py. \e[m\n"
         let COUNTER_ERROR++,FILE_COUNTER_ERROR++
      fi

      if grep -iq "import pandas" $file
      then
         printf "\e[0;31m  - Databricks notebook ERROR: El uso de pandas debe ser por medio de la api de pyspark: pyspark.pandas. \e[m\n"
         let COUNTER_ERROR++,FILE_COUNTER_ERROR++
      fi 
   
   fi

   if [ "$FILE_COUNTER_ERROR" -eq "0" ]; 
   then
      printf "\e[0;32m  - No se encontraron errores \e[m\n"
   fi

done

if [ $FILE_COUNTER -gt 20 ]; 
then
   printf '\n\e[0;33mGeneral ERROR: Los Pull Request deben tener como maximo 20 archivos.\e[m\n'
fi

if [ "$COUNTER_ERROR" -ne "0" ]; 
then
   printf '\n\e[0;31mSe han encontrado %d errores!!!\e[m\n' $COUNTER_ERROR
   # exit 1
else
   printf '\n\e[0;32m%-6s\e[m\n' " Todos los archivos pasaron las validaciones preliminares"
fi


# files_deleted=`git diff origin/master... --name-only --diff-filter=D`

# printf "\n\e[0;36m ********* Archivos Eliminados ********* \e[m\n\n"

# IFS=$'\n'
# for file in $files_deleted; do
#    echo $file
# done
IFS=$IFS_SAVE  # Restaurar el valor original de IFS