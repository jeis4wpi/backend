import configurations from '../configurations';
import '../extensions';
// TODO change logger to just use console in this case
import logger from '../utilities/logger';
import '../global-error-handlers';

import { Prisma, PrismaClient } from '@prisma/client';

// import * as pugPlay from './playground-pug';
// import * as rendererPlay from './playground-renderer-functions';
// import * as schedulerPlay from './playground-scheduler-functions';

const enabledMarker = new Array(20).join('*');
const disabledMarker = new Array(20).join('#');
if (configurations.email.enabled) {
    logger.info(`${enabledMarker} EMAIL ENABLED ${enabledMarker}`);
} else {
    logger.info(`${disabledMarker} EMAIL DISABLED ${disabledMarker}`);
}

import { sync } from '../database';

async function main(prisma: PrismaClient<Prisma.PrismaClientOptions, never>): Promise<void> {
    const users = await prisma.users.findMany({
        where: {
            university: {
                // eslint-disable-next-line @typescript-eslint/camelcase
                university_id: 4,
            }
        },
        include: {
            course: true
        },
    });
    console.log(users);
}

(async (): Promise<void> => {
    try {
        await sync();
        logger.info('Playground start');
        const prisma = new PrismaClient();

        main(prisma)
          .catch(e => {
            throw e;
          })
          .finally(async () => {
            await prisma.$disconnect();
          });
        logger.info('Playground done');
    } catch (e) {
        logger.error('Could not start up', e);
        // Used a larger number so that we could determine by the error code that this was an application error
        process.exit(87);
    }
})();
