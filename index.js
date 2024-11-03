const express = require('express');
const { Pool } = require('pg');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');

const app = express();

// Configuración de la conexión con PostgreSQL
const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'hr',
    password: 'utm1234', // Asegúrate de que esta sea la contraseña correcta
    port: 5432,
});

// ENDPOINT 1: Subir CSV y cargarlo en PostgreSQL
app.post('/upload-csv', async (req, res) => {
    const filePath = "C:/Users/Reyner/csv_postgres_api/uploads/countries.csv"; // Ruta del archivo CSV
    const results = [];

    // Verifica si el archivo existe
    if (!fs.existsSync(filePath)) {
        console.error('El archivo no existe en la ruta especificada:', filePath);
        return res.status(400).json({ error: 'El archivo no existe en la ruta especificada.' });
    }

    fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (data) => {
            if (data.country_id) { // Verifica que country_id no esté vacío
                results.push(data);
            } else {
                console.warn('Fila ignorada debido a country_id vacío:', data);
            }
        })
        .on('end', async () => {
            const insertPromises = results.map(async (row) => {
                try {
                    console.log('Insertando fila:', row); // Imprime la fila que se va a insertar
                    await pool.query(
                        'INSERT INTO public.countries (country_id, country_name, region_id) VALUES ($1, $2, $3)',
                        [row.country_id, row.country_name, row.region_id]
                    );
                } catch (error) {
                    if (error.code === '23505') { // Código de error para violación de unicidad
                        console.warn('Registro duplicado ignorado:', row);
                    } else {
                        console.error('Error al insertar datos en la base de datos:', error.message);
                    }
                }
            });

            await Promise.all(insertPromises); // Espera a que todas las inserciones terminen
            res.status(200).json({ message: 'CSV procesado. Inserciones completadas.' });
        });
});

// ENDPOINT 2: Exportar datos de PostgreSQL a CSV
app.get('/download-csv', async (req, res) => {
    try {
        const { rows } = await pool.query('SELECT country_id, country_name, region_id FROM public.countries');
        const csvData = [
            ['country_id', 'country_name', 'region_id'],
            ...rows.map((row) => [row.country_id, row.country_name, row.region_id]),
        ];

        const filePath = path.join(__dirname, 'downloads', 'countries.csv');
        const writeStream = fs.createWriteStream(filePath);

        csvData.forEach((row) => {
            writeStream.write(row.join(',') + '\n');
        });

        writeStream.end();
        writeStream.on('finish', () => {
            res.download(filePath, 'countries.csv', (err) => {
                if (err) {
                    console.error('Error al descargar el CSV:', err);
                    res.status(500).json({ error: 'Error al descargar el CSV.' });
                } else {
                    fs.unlinkSync(filePath); // Elimina el archivo después de descargarlo
                }
            });
        });
    } catch (error) {
        console.error('Error al generar el CSV desde la base de datos:', error);
        res.status(500).json({ error: 'Error al generar el CSV desde la base de datos.' });
    }
});

// Inicia el servidor
const PORT = 3000;
app.listen(PORT, () => {
    console.log(`Servidor ejecutándose en http://localhost:${PORT}`);
});
