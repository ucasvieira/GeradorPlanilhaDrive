require('dotenv').config();
const { Client } = require('pg');
const { Workbook } = require('exceljs');
const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');

async function generateSpreadsheet() {
    const client = new Client({
        user: process.env.DB_USER,
        host: process.env.DB_HOST,
        database: process.env.DB_DATABASE,
        password: process.env.DB_PASSWORD,
        port: process.env.DB_PORT,
    });

    try {
        const now = new Date();
        const oneMonthAgo = new Date();
        oneMonthAgo.setMonth(now.getMonth() - 1);

        const startDate = oneMonthAgo.toISOString().split('T')[0];
        const endDate = now.toISOString().split('T')[0];
        
        try {
            await client.connect();
        } catch (err) {
            console.error('Erro ao conectar ao banco de dados:', err);
            return;
        }
        console.log(`Conectado ao banco de dados ${process.env.DB_DATABASE} com sucesso.`);
        const queryFilePath = path.join(__dirname, process.env.QUERY_FILE);
        const query = fs.readFileSync(queryFilePath, 'utf8');

        const values = [startDate, endDate];
        const result = await client.query(query, values);

        if (!result.rows || result.rows.length === 0) {
            console.log('Sem dados para o periodo especificado.');
            return;
        }

        const workbook = new Workbook();
        const worksheet = workbook.addWorksheet('Data');

        const columns = Object.keys(result.rows[0]).map((key) => ({ header: key, key }));
        worksheet.columns = columns;

        result.rows.forEach((row) => {
            worksheet.addRow(row);
        });

        const fileName = `planilha${startDate}a${endDate}.xlsx`;
        const filePath = path.join(__dirname, fileName);
        await workbook.xlsx.writeFile(filePath);
        console.log(`Planilha salva em ${filePath}`);
        await uploadToGoogleDrive(filePath, fileName);
    } catch (err) {
        console.error('Erro:', err);
    } finally {
        await client.end();
    }
}

async function uploadToGoogleDrive(filePath, fileName) {
    try {
        const auth = new google.auth.GoogleAuth({
            keyFile: process.env.GOOGLE_KEY_FILE,
            scopes: ['https://www.googleapis.com/auth/drive.file'],
        });

        const drive = google.drive({ version: 'v3', auth });

        const folderId = process.env.GOOGLE_DRIVE_FOLDER_ID;

        const fileMetadata = {
            name: fileName,
            parents: [folderId],
        };
        const media = {
            mimeType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            body: fs.createReadStream(filePath),
        };

        const response = await drive.files.create({
            resource: fileMetadata,
            media: media,
            fields: 'id',
        });

        if (response && response.data && response.data.id) {
            console.log(`Planilha upada no Drive com ID: ${response.data.id}`);

            fs.unlink(filePath, (err) => {
                if (err) {
                    console.error(`Falha ao deletar arquivo local: ${filePath}`, err);
                } else {
                    console.log(`Arquivo local deletado: ${filePath}`);
                }
            });
        } else {
            throw new Error('Falha ao upar arquivo no Drive. Nenhum ID retornado.');
        }
    } catch (err) {
        console.error('Falha ao upar arquivo no Drive:', err);
    }
}

generateSpreadsheet();