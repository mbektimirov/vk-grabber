const csv = require('fast-csv');
const fs = require('fs');
const superagent = require('superagent');
const flatMap = require('lodash/flatMap');
const download = require('image-downloader');
const mkdirp = require('mkdirp');

const rows = [];

fs
  .createReadStream('report.csv')
  .pipe(csv({ headers: true }))
  .on('data', data => {
    rows.push(data);
  })
  .on('end', start);

function start() {
  // for (row of rows) {
  //   const photos = await downloadPhotos(row.url);
  // }

  const newRows = rows
    .map(row => {
      const matched = row.url.match(/wall(-?\d*_\d*)/);

      return (
        matched && {
          postId: matched[1],
          describe: row,
        }
      );
    })
    .filter(id => id);

  const groupedRows = [];

  // group ids to reduce api requests
  // max 100 posts per request
  while (newRows.length > 0) {
    groupedRows.push(newRows.splice(0, 50)); // put 50 for memory efficiency
  }

  downloadPostImages(groupedRows);
}

function timeout(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const ACCESS_TOKEN = fs.readFileSync('vk_token', 'utf-8').trim();

async function downloadPostImages(groupedRows) {
  let photoUrls = [];

  for (let rowGroup of groupedRows) {
    // console.log('PROCESSING...');

    const ids = rowGroup.map(({ postId }) => postId).join(',');

    const resp = await superagent.get(
      'https://api.vk.com/method/wall.getById?&v=5.68',
      {
        posts: ids,
        access_token: ACCESS_TOKEN,
      }
    );

    const photoGroup = {};

    resp.body.response.forEach(post => {
      const { attachments, id, owner_id } = post;

      if (!attachments) {
        return;
      }

      const hiResPhotos = attachments
        .map(p => {
          if (p.type === 'photo') {
            // choose maximal resolution
            const [maxResolutionKey] = Object.keys(p.photo)
              .reduce((res, key) => {
                key.startsWith('photo_') && res.push(key);
                return res;
              }, [])
              .sort(
                (x, y) => Number(y.match(/\d+/)[0]) - Number(x.match(/\d+/)[0])
              );

            return p.photo[maxResolutionKey];
          }
        })
        .filter(id => id);

      photoGroup[`${owner_id}_${id}`] = hiResPhotos;
    });

    for (let row of rowGroup) {
      const { postId, describe } = row;
      const photoUrls = photoGroup[postId];

      if (!photoUrls) {
        continue;
      }

      const dest = `dataset/${postId}`;
      const describePath = `${dest}/describe.json`;

      mkdirp.sync(dest);

      if (!fs.existsSync(describePath)) {
        console.log('Writing descrption to', describePath);

        fs.writeFileSync(describePath, JSON.stringify(describe, null, 2));
      }

      console.log('Downloading images for post', postId);

      Promise.all(
        photoUrls.map(url => {
          const [fileName] = url.split('/').reverse();

          if (!fs.existsSync(`${dest}/${fileName}`)) {
            console.log('\tDownloading...', url);

            return download.image({ url, dest });
          } else {
            return `Skipping ${dest}/${fileName}`;
          }
        })
      ).then(r => console.log('Done images for', postId, r));
    }

    // await timeout(1000);
  }
}
